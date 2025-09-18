#!/usr/bin/env python3
"""
Trino Performance Analyzer

这个脚本提供了一套完整的Trino性能分析和监控工具，包括：
- 自动化性能报告生成
- 查询性能趋势分析  
- 统计信息健康度检查
- 优化建议生成
- 性能预警系统

使用方法:
    python trino_performance_analyzer.py --config config.yaml
    python trino_performance_analyzer.py --report --days 7
    python trino_performance_analyzer.py --analyze-slow-queries --threshold 300
"""

import argparse
import json
import logging
import sys
import time
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import trino
from trino.auth import BasicAuthentication

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trino_analyzer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TrinoPerformanceAnalyzer:
    """Trino性能分析器主类"""
    
    def __init__(self, config_path: str = None):
        """初始化分析器"""
        self.config = self._load_config(config_path)
        self.conn = self._create_connection()
        
    def _load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        default_config = {
            'trino': {
                'host': 'localhost',
                'port': 8080,
                'user': 'admin',
                'catalog': 'system',
                'schema': 'runtime'
            },
            'analysis': {
                'slow_query_threshold': 300,  # 5分钟
                'memory_threshold_gb': 10,
                'queue_time_threshold': 30,
                'failure_rate_threshold': 0.05
            },
            'reporting': {
                'output_dir': './reports',
                'days_lookback': 7
            }
        }
        
        if config_path:
            try:
                with open(config_path, 'r') as f:
                    user_config = yaml.safe_load(f)
                    default_config.update(user_config)
            except Exception as e:
                logger.warning(f"无法加载配置文件 {config_path}: {e}，使用默认配置")
                
        return default_config
        
    def _create_connection(self):
        """创建Trino连接"""
        config = self.config['trino']
        
        auth = None
        if config.get('username') and config.get('password'):
            auth = BasicAuthentication(config['username'], config['password'])
            
        return trino.dbapi.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            catalog=config['catalog'],
            schema=config['schema'],
            auth=auth
        )
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            logger.error(f"查询内容: {query[:200]}...")
            raise
    
    def generate_performance_report(self, days: int = 7) -> Dict:
        """生成性能报告"""
        logger.info(f"生成过去 {days} 天的性能报告...")
        
        report = {
            'report_time': datetime.now().isoformat(),
            'analysis_period_days': days,
            'summary': {},
            'detailed_analysis': {}
        }
        
        # 基础统计
        report['summary'] = self._get_basic_stats(days)
        
        # 慢查询分析
        report['detailed_analysis']['slow_queries'] = self._analyze_slow_queries(days)
        
        # 资源使用分析
        report['detailed_analysis']['resource_usage'] = self._analyze_resource_usage(days)
        
        # 统计信息健康度
        report['detailed_analysis']['stats_health'] = self._check_stats_health()
        
        # 优化建议
        report['recommendations'] = self._generate_recommendations(report)
        
        return report
    
    def _get_basic_stats(self, days: int) -> Dict:
        """获取基础统计信息"""
        query = f"""
        SELECT 
            COUNT(*) as total_queries,
            COUNT(CASE WHEN state = 'FINISHED' THEN 1 END) as successful_queries,
            COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as failed_queries,
            AVG(elapsed_time.seconds) as avg_runtime_seconds,
            PERCENTILE(elapsed_time.seconds, 0.95) as p95_runtime_seconds,
            SUM(total_bytes) / 1024.0 / 1024.0 / 1024.0 / 1024.0 as total_processed_tb,
            AVG(peak_user_memory_bytes) / 1024.0 / 1024.0 / 1024.0 as avg_memory_gb,
            MAX(peak_user_memory_bytes) / 1024.0 / 1024.0 / 1024.0 as max_memory_gb
        FROM system.runtime.queries
        WHERE created >= current_timestamp - INTERVAL '{days}' DAY
        """
        
        result = self.execute_query(query)
        if not result.empty:
            stats = result.iloc[0].to_dict()
            stats['success_rate'] = (stats['successful_queries'] / 
                                   max(stats['total_queries'], 1))
            stats['failure_rate'] = (stats['failed_queries'] / 
                                   max(stats['total_queries'], 1))
            return stats
        return {}
    
    def _analyze_slow_queries(self, days: int) -> Dict:
        """分析慢查询"""
        threshold = self.config['analysis']['slow_query_threshold']
        
        query = f"""
        WITH slow_queries AS (
            SELECT 
                query_id,
                user_name,
                query,
                elapsed_time.seconds as total_seconds,
                queued_time.seconds as queued_seconds,
                planning_time.seconds as planning_seconds,
                execution_time.seconds as execution_seconds,
                total_bytes / 1024.0 / 1024.0 / 1024.0 as processed_gb,
                peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_memory_gb,
                created
            FROM system.runtime.queries
            WHERE state = 'FINISHED'
              AND elapsed_time.seconds > {threshold}
              AND created >= current_timestamp - INTERVAL '{days}' DAY
        )
        SELECT 
            COUNT(*) as slow_query_count,
            AVG(total_seconds) as avg_slow_runtime,
            AVG(planning_seconds) as avg_planning_time,
            AVG(execution_seconds) as avg_execution_time,
            AVG(processed_gb) as avg_data_processed,
            AVG(peak_memory_gb) as avg_memory_usage,
            array_agg(DISTINCT user_name) as affected_users
        FROM slow_queries
        """
        
        result = self.execute_query(query)
        
        if not result.empty and result.iloc[0]['slow_query_count'] > 0:
            analysis = result.iloc[0].to_dict()
            
            # 获取具体的慢查询详情
            detail_query = f"""
            SELECT 
                query_id,
                user_name,
                left(query, 200) as query_preview,
                elapsed_time.seconds as total_seconds,
                total_bytes / 1024.0 / 1024.0 / 1024.0 as processed_gb,
                peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_memory_gb
            FROM system.runtime.queries
            WHERE state = 'FINISHED'
              AND elapsed_time.seconds > {threshold}
              AND created >= current_timestamp - INTERVAL '{days}' DAY
            ORDER BY elapsed_time.seconds DESC
            LIMIT 10
            """
            
            details = self.execute_query(detail_query)
            analysis['top_slow_queries'] = details.to_dict('records')
            
            return analysis
        
        return {'slow_query_count': 0}
    
    def _analyze_resource_usage(self, days: int) -> Dict:
        """分析资源使用情况"""
        query = f"""
        WITH hourly_stats AS (
            SELECT 
                date_trunc('hour', created) as hour,
                COUNT(*) as query_count,
                AVG(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as avg_memory_gb,
                MAX(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as max_memory_gb,
                AVG(queued_time.seconds) as avg_queue_time,
                COUNT(CASE WHEN error_code = 'EXCEEDED_MEMORY_LIMIT' THEN 1 END) as oom_count
            FROM system.runtime.queries
            WHERE created >= current_timestamp - INTERVAL '{days}' DAY
            GROUP BY 1
        )
        SELECT 
            AVG(query_count) as avg_queries_per_hour,
            MAX(query_count) as peak_queries_per_hour,
            AVG(avg_memory_gb) as avg_memory_usage_gb,
            MAX(max_memory_gb) as peak_memory_usage_gb,
            AVG(avg_queue_time) as avg_queue_time_seconds,
            SUM(oom_count) as total_oom_failures
        FROM hourly_stats
        """
        
        result = self.execute_query(query)
        if not result.empty:
            return result.iloc[0].to_dict()
        return {}
    
    def _check_stats_health(self) -> Dict:
        """检查统计信息健康度"""
        query = """
        WITH table_stats AS (
            SELECT 
                COUNT(*) as total_tables,
                COUNT(CASE WHEN row_count IS NULL THEN 1 END) as missing_row_stats,
                COUNT(CASE WHEN row_count = 0 THEN 1 END) as zero_row_stats
            FROM system.metadata.table_statistics
            WHERE table_schema NOT IN ('information_schema', 'system')
        ),
        column_stats AS (
            SELECT 
                COUNT(*) as total_columns,
                COUNT(CASE WHEN distinct_values_count IS NULL THEN 1 END) as missing_ndv_stats,
                COUNT(CASE WHEN nulls_fraction IS NULL THEN 1 END) as missing_null_stats
            FROM system.metadata.column_statistics
            WHERE table_schema NOT IN ('information_schema', 'system')
        )
        SELECT 
            t.total_tables,
            t.missing_row_stats,
            t.zero_row_stats,
            c.total_columns,
            c.missing_ndv_stats,
            c.missing_null_stats
        FROM table_stats t, column_stats c
        """
        
        result = self.execute_query(query)
        if not result.empty:
            stats = result.iloc[0].to_dict()
            
            # 计算健康度百分比
            stats['table_stats_health'] = (
                1 - (stats['missing_row_stats'] + stats['zero_row_stats']) / 
                max(stats['total_tables'], 1)
            ) * 100
            
            stats['column_stats_health'] = (
                1 - (stats['missing_ndv_stats'] + stats['missing_null_stats']) / 
                max(stats['total_columns'], 1)
            ) * 100
            
            return stats
        return {}
    
    def _generate_recommendations(self, report: Dict) -> List[Dict]:
        """基于分析结果生成优化建议"""
        recommendations = []
        
        summary = report.get('summary', {})
        slow_queries = report.get('detailed_analysis', {}).get('slow_queries', {})
        resource_usage = report.get('detailed_analysis', {}).get('resource_usage', {})
        stats_health = report.get('detailed_analysis', {}).get('stats_health', {})
        
        # 失败率建议
        failure_rate = summary.get('failure_rate', 0)
        if failure_rate > self.config['analysis']['failure_rate_threshold']:
            recommendations.append({
                'category': 'stability',
                'priority': 'high',
                'issue': f'查询失败率过高: {failure_rate:.2%}',
                'recommendation': '检查集群资源配置，分析失败原因分布',
                'action': 'SELECT error_type, COUNT(*) FROM system.runtime.queries WHERE state = \'FAILED\' GROUP BY error_type'
            })
        
        # 慢查询建议
        slow_count = slow_queries.get('slow_query_count', 0)
        if slow_count > 0:
            recommendations.append({
                'category': 'performance',
                'priority': 'medium',
                'issue': f'发现 {slow_count} 个慢查询',
                'recommendation': '分析慢查询的执行计划，考虑启用动态过滤或调整Join策略',
                'action': 'SET SESSION enable_dynamic_filtering = true; SET SESSION join_distribution_type = \'AUTOMATIC\''
            })
        
        # 内存使用建议
        avg_memory = resource_usage.get('avg_memory_usage_gb', 0)
        peak_memory = resource_usage.get('peak_memory_usage_gb', 0)
        if peak_memory > self.config['analysis']['memory_threshold_gb']:
            recommendations.append({
                'category': 'resource',
                'priority': 'medium', 
                'issue': f'内存使用过高，峰值: {peak_memory:.1f}GB',
                'recommendation': '考虑调整query_max_memory参数或优化内存密集型查询',
                'action': 'SET SESSION query_max_memory_per_node = \'8GB\''
            })
        
        # 统计信息建议
        table_health = stats_health.get('table_stats_health', 100)
        if table_health < 80:
            recommendations.append({
                'category': 'statistics',
                'priority': 'high',
                'issue': f'表统计信息缺失严重，健康度: {table_health:.1f}%',
                'recommendation': '立即收集关键表的统计信息以改善CBO效果',
                'action': '执行 ANALYZE TABLE 语句收集统计信息'
            })
        
        return recommendations
    
    def create_performance_dashboard(self, days: int = 7) -> str:
        """创建性能仪表板"""
        logger.info("创建性能仪表板...")
        
        # 获取时间序列数据
        query = f"""
        SELECT 
            date_trunc('hour', created) as hour,
            COUNT(*) as query_count,
            AVG(elapsed_time.seconds) as avg_runtime,
            PERCENTILE(elapsed_time.seconds, 0.95) as p95_runtime,
            AVG(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as avg_memory_gb,
            COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as failed_count
        FROM system.runtime.queries
        WHERE created >= current_timestamp - INTERVAL '{days}' DAY
        GROUP BY 1
        ORDER BY 1
        """
        
        df = self.execute_query(query)
        
        if df.empty:
            logger.warning("没有查询数据可用于生成仪表板")
            return ""
        
        # 创建子图
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=('查询数量趋势', '运行时间趋势', 
                          '内存使用趋势', '失败率趋势',
                          '95分位运行时间', '查询数量分布'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"type": "bar"}]]
        )
        
        # 查询数量趋势
        fig.add_trace(
            go.Scatter(x=df['hour'], y=df['query_count'], 
                      mode='lines+markers', name='查询数量'),
            row=1, col=1
        )
        
        # 运行时间趋势  
        fig.add_trace(
            go.Scatter(x=df['hour'], y=df['avg_runtime'], 
                      mode='lines+markers', name='平均运行时间(秒)'),
            row=1, col=2
        )
        
        # 内存使用趋势
        fig.add_trace(
            go.Scatter(x=df['hour'], y=df['avg_memory_gb'], 
                      mode='lines+markers', name='平均内存使用(GB)'),
            row=2, col=1
        )
        
        # 失败率趋势
        df['failure_rate'] = df['failed_count'] / df['query_count'] * 100
        fig.add_trace(
            go.Scatter(x=df['hour'], y=df['failure_rate'], 
                      mode='lines+markers', name='失败率(%)'),
            row=2, col=2  
        )
        
        # 95分位运行时间
        fig.add_trace(
            go.Scatter(x=df['hour'], y=df['p95_runtime'], 
                      mode='lines+markers', name='P95运行时间(秒)'),
            row=3, col=1
        )
        
        # 查询数量分布柱状图
        fig.add_trace(
            go.Bar(x=df['hour'].dt.hour, y=df['query_count'], 
                   name='按小时分布'),
            row=3, col=2
        )
        
        fig.update_layout(
            height=1200, 
            title_text=f"Trino性能仪表板 (过去{days}天)",
            showlegend=False
        )
        
        # 保存仪表板
        output_file = f"trino_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        fig.write_html(output_file)
        
        logger.info(f"仪表板已保存到: {output_file}")
        return output_file
    
    def analyze_query_patterns(self, days: int = 7) -> Dict:
        """分析查询模式"""
        logger.info("分析查询模式...")
        
        query = f"""
        WITH query_patterns AS (
            SELECT 
                regexp_replace(
                    regexp_replace(query, '\\d+', 'N'),  -- 数字替换
                    '''[^'']*''', '''X'''                -- 字符串替换  
                ) as pattern,
                elapsed_time.seconds as seconds,
                user_name,
                created
            FROM system.runtime.queries
            WHERE state = 'FINISHED'
              AND created >= current_timestamp - INTERVAL '{days}' DAY
              AND length(query) > 50  -- 过滤简单查询
        )
        SELECT 
            left(pattern, 200) as pattern_preview,
            COUNT(*) as execution_count,
            AVG(seconds) as avg_runtime,
            STDDEV(seconds) as stddev_runtime,
            MIN(seconds) as min_runtime,
            MAX(seconds) as max_runtime,
            array_agg(DISTINCT user_name) as users
        FROM query_patterns
        GROUP BY pattern
        HAVING COUNT(*) >= 3
        ORDER BY execution_count DESC, avg_runtime DESC
        LIMIT 20
        """
        
        result = self.execute_query(query)
        
        patterns = []
        for _, row in result.iterrows():
            pattern_info = {
                'pattern': row['pattern_preview'],
                'frequency': row['execution_count'],
                'avg_runtime': row['avg_runtime'],
                'runtime_variability': (row['stddev_runtime'] / max(row['avg_runtime'], 1)),
                'users': row['users'],
                'optimization_potential': 'high' if row['avg_runtime'] > 300 else 'medium' if row['avg_runtime'] > 60 else 'low'
            }
            patterns.append(pattern_info)
        
        return {'patterns': patterns, 'total_patterns': len(patterns)}
    
    def export_report(self, report: Dict, format: str = 'json') -> str:
        """导出报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format.lower() == 'json':
            filename = f"trino_performance_report_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
                
        elif format.lower() == 'html':
            filename = f"trino_performance_report_{timestamp}.html"
            html_content = self._generate_html_report(report)
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
                
        logger.info(f"报告已导出到: {filename}")
        return filename
    
    def _generate_html_report(self, report: Dict) -> str:
        """生成HTML格式的报告"""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Trino Performance Report</title>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
                .section {{ margin: 20px 0; }}
                .metric {{ background: #e8f4fd; padding: 10px; margin: 5px 0; border-left: 4px solid #2196F3; }}
                .recommendation {{ background: #fff3cd; padding: 10px; margin: 5px 0; border-left: 4px solid #ffc107; }}
                .high-priority {{ border-left-color: #dc3545 !important; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Trino 性能分析报告</h1>
                <p>报告时间: {report['report_time']}</p>
                <p>分析周期: {report['analysis_period_days']} 天</p>
            </div>
            
            <div class="section">
                <h2>📊 基础统计</h2>
        """
        
        summary = report.get('summary', {})
        for key, value in summary.items():
            if isinstance(value, float):
                if key.endswith('_rate'):
                    value = f"{value:.2%}"
                elif key.endswith('_gb') or key.endswith('_tb'):
                    value = f"{value:.2f}"
                else:
                    value = f"{value:.2f}"
            html += f'<div class="metric"><strong>{key}:</strong> {value}</div>\n'
        
        html += """
            </div>
            
            <div class="section">
                <h2>⚡ 优化建议</h2>
        """
        
        for rec in report.get('recommendations', []):
            priority_class = ' high-priority' if rec.get('priority') == 'high' else ''
            html += f"""
            <div class="recommendation{priority_class}">
                <h4>{rec.get('issue', 'Unknown Issue')}</h4>
                <p><strong>建议:</strong> {rec.get('recommendation', '')}</p>
                <p><strong>操作:</strong> <code>{rec.get('action', '')}</code></p>
            </div>
            """
        
        html += """
            </div>
        </body>
        </html>
        """
        
        return html


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Trino Performance Analyzer')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--report', action='store_true', help='生成性能报告')
    parser.add_argument('--dashboard', action='store_true', help='创建性能仪表板')
    parser.add_argument('--analyze-patterns', action='store_true', help='分析查询模式')
    parser.add_argument('--days', type=int, default=7, help='分析天数')
    parser.add_argument('--format', choices=['json', 'html'], default='json', help='报告格式')
    parser.add_argument('--threshold', type=int, default=300, help='慢查询阈值(秒)')
    
    args = parser.parse_args()
    
    try:
        analyzer = TrinoPerformanceAnalyzer(args.config)
        
        if args.report:
            logger.info("开始生成性能报告...")
            report = analyzer.generate_performance_report(args.days)
            filename = analyzer.export_report(report, args.format)
            logger.info(f"性能报告生成完成: {filename}")
            
        if args.dashboard:
            logger.info("开始创建性能仪表板...")
            dashboard_file = analyzer.create_performance_dashboard(args.days)
            logger.info(f"性能仪表板创建完成: {dashboard_file}")
            
        if args.analyze_patterns:
            logger.info("开始分析查询模式...")
            patterns = analyzer.analyze_query_patterns(args.days)
            logger.info(f"发现 {patterns['total_patterns']} 个查询模式")
            
            # 输出高频查询模式
            for pattern in patterns['patterns'][:5]:
                logger.info(f"模式频率: {pattern['frequency']}, 平均运行时间: {pattern['avg_runtime']:.1f}秒")
                logger.info(f"查询模式: {pattern['pattern'][:100]}...")
                
    except Exception as e:
        logger.error(f"分析过程中出现错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
