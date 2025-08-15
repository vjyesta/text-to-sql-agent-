"""
Result formatting utilities.
Handles formatting of query results, data exports, and display formatting.
"""

import json
import csv
import io
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, date
from decimal import Decimal
from tabulate import tabulate
import logging

logger = logging.getLogger(__name__)


class ResultFormatter:
    """
    Formats query results for different output formats and display styles.
    """
    
    def __init__(self, max_column_width: int = 50, max_rows_display: int = 100):
        """
        Initialize the result formatter.
        
        Args:
            max_column_width: Maximum width for column display
            max_rows_display: Maximum rows to display in terminal
        """
        self.max_column_width = max_column_width
        self.max_rows_display = max_rows_display
        
        # Table format styles
        self.table_formats = {
            'grid': 'grid',
            'simple': 'simple',
            'fancy': 'fancy_grid',
            'html': 'html',
            'markdown': 'pipe',
            'csv': 'csv',
            'json': 'json'
        }
    
    def format_query_results(self, 
                            columns: List[str], 
                            data: List[Tuple],
                            format_type: str = 'grid',
                            show_stats: bool = True) -> str:
        """
        Format query results for display.
        
        Args:
            columns: Column names
            data: Query result data
            format_type: Output format type
            show_stats: Whether to show statistics
            
        Returns:
            Formatted string representation
        """
        if not data:
            return "No results found."
        
        # Truncate data if too many rows
        truncated = False
        if len(data) > self.max_rows_display:
            display_data = data[:self.max_rows_display]
            truncated = True
        else:
            display_data = data
        
        # Format based on type
        if format_type == 'json':
            result = self._format_as_json(columns, display_data)
        elif format_type == 'csv':
            result = self._format_as_csv(columns, display_data)
        elif format_type == 'html':
            result = self._format_as_html(columns, display_data)
        else:
            result = self._format_as_table(columns, display_data, format_type)
        
        # Add statistics if requested
        if show_stats:
            stats = self._generate_statistics(columns, data)
            result = f"{result}\n\n{stats}"
        
        # Add truncation notice
        if truncated:
            result += f"\n\n⚠️ Showing first {self.max_rows_display} of {len(data)} total rows"
        
        return result
    
    def _format_as_table(self, 
                        columns: List[str], 
                        data: List[Tuple],
                        style: str = 'grid') -> str:
        """
        Format data as an ASCII table.
        
        Args:
            columns: Column names
            data: Data rows
            style: Table style
            
        Returns:
            Formatted table string
        """
        # Process data for display
        processed_data = []
        for row in data:
            processed_row = []
            for value in row:
                processed_value = self._format_value(value)
                # Truncate long values
                if len(str(processed_value)) > self.max_column_width:
                    processed_value = str(processed_value)[:self.max_column_width-3] + "..."
                processed_row.append(processed_value)
            processed_data.append(processed_row)
        
        # Get the appropriate tabulate format
        table_format = self.table_formats.get(style, 'grid')
        
        return tabulate(
            processed_data,
            headers=columns,
            tablefmt=table_format,
            floatfmt='.2f',
            numalign='right',
            stralign='left'
        )
    
    def _format_as_json(self, columns: List[str], data: List[Tuple]) -> str:
        """
        Format data as JSON.
        
        Args:
            columns: Column names
            data: Data rows
            
        Returns:
            JSON string
        """
        result = []
        for row in data:
            row_dict = {}
            for col, val in zip(columns, row):
                row_dict[col] = self._serialize_value(val)
            result.append(row_dict)
        
        return json.dumps(result, indent=2, default=str)
    
    def _format_as_csv(self, columns: List[str], data: List[Tuple]) -> str:
        """
        Format data as CSV.
        
        Args:
            columns: Column names
            data: Data rows
            
        Returns:
            CSV string
        """
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(columns)
        
        # Write data
        for row in data:
            writer.writerow([self._format_value(val) for val in row])
        
        return output.getvalue()
    
    def _format_as_html(self, columns: List[str], data: List[Tuple]) -> str:
        """
        Format data as HTML table.
        
        Args:
            columns: Column names
            data: Data rows
            
        Returns:
            HTML table string
        """
        html = ['<table border="1" class="dataframe">']
        
        # Add header
        html.append('<thead><tr>')
        for col in columns:
            html.append(f'<th>{col}</th>')
        html.append('</tr></thead>')
        
        # Add body
        html.append('<tbody>')
        for row in data:
            html.append('<tr>')
            for value in row:
                formatted_value = self._format_value(value)
                html.append(f'<td>{formatted_value}</td>')
            html.append('</tr>')
        html.append('</tbody>')
        
        html.append('</table>')
        
        return '\n'.join(html)
    
    def _format_value(self, value: Any) -> str:
        """
        Format a single value for display.
        
        Args:
            value: Value to format
            
        Returns:
            Formatted string
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, bool):
            return '✓' if value else '✗'
        elif isinstance(value, (int, float, Decimal)):
            if isinstance(value, float) or isinstance(value, Decimal):
                return f"{value:.2f}"
            return str(value)
        elif isinstance(value, (datetime, date)):
            if isinstance(value, datetime):
                return value.strftime('%Y-%m-%d %H:%M:%S')
            return value.strftime('%Y-%m-%d')
        else:
            return str(value)
    
    def _serialize_value(self, value: Any) -> Any:
        """
        Serialize a value for JSON export.
        
        Args:
            value: Value to serialize
            
        Returns:
            Serializable value
        """
        if value is None:
            return None
        elif isinstance(value, (bool, int, str)):
            return value
        elif isinstance(value, (float, Decimal)):
            return float(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        else:
            return str(value)
    
    def _generate_statistics(self, columns: List[str], data: List[Tuple]) -> str:
        """
        Generate statistics for the result set.
        
        Args:
            columns: Column names
            data: Data rows
            
        Returns:
            Statistics string
        """
        stats = []
        stats.append("📊 Result Statistics:")
        stats.append(f"  • Total Rows: {len(data)}")
        stats.append(f"  • Total Columns: {len(columns)}")
        
        # Analyze numeric columns
        numeric_stats = self._analyze_numeric_columns(columns, data)
        if numeric_stats:
            stats.append("  • Numeric Columns:")
            for col_name, col_stats in numeric_stats.items():
                stats.append(f"    - {col_name}:")
                stats.append(f"      Min: {col_stats['min']:.2f}, Max: {col_stats['max']:.2f}")
                stats.append(f"      Avg: {col_stats['avg']:.2f}, Sum: {col_stats['sum']:.2f}")
        
        return '\n'.join(stats)
    
    def _analyze_numeric_columns(self, columns: List[str], data: List[Tuple]) -> Dict[str, Dict[str, float]]:
        """
        Analyze numeric columns for statistics.
        
        Args:
            columns: Column names
            data: Data rows
            
        Returns:
            Dictionary of statistics per numeric column
        """
        numeric_stats = {}
        
        for i, col in enumerate(columns):
            values = []
            for row in data:
                value = row[i]
                if isinstance(value, (int, float, Decimal)) and value is not None:
                    values.append(float(value))
            
            if values:
                numeric_stats[col] = {
                    'min': min(values),
                    'max': max(values),
                    'avg': sum(values) / len(values),
                    'sum': sum(values)
                }
        
        return numeric_stats
    
    def format_error(self, error: str, suggestion: Optional[str] = None) -> str:
        """
        Format an error message with optional suggestion.
        
        Args:
            error: Error message
            suggestion: Optional suggestion for fixing
            
        Returns:
            Formatted error string
        """
        output = [f"❌ Error: {error}"]
        
        if suggestion:
            output.append(f"\n💡 Suggestion: {suggestion}")
        
        return '\n'.join(output)
    
    def format_success(self, message: str, details: Optional[Dict[str, Any]] = None) -> str:
        """
        Format a success message with optional details.
        
        Args:
            message: Success message
            details: Optional additional details
            
        Returns:
            Formatted success string
        """
        output = [f"✅ Success: {message}"]
        
        if details:
            output.append("\nDetails:")
            for key, value in details.items():
                output.append(f"  • {key}: {value}")
        
        return '\n'.join(output)
    
    def export_to_file(self, 
                      columns: List[str], 
                      data: List[Tuple],
                      filename: str,
                      format_type: str = 'csv') -> bool:
        """
        Export query results to a file.
        
        Args:
            columns: Column names
            data: Query results
            filename: Output filename
            format_type: Export format (csv, json, html)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if format_type == 'json':
                content = self._format_as_json(columns, data)
            elif format_type == 'csv':
                content = self._format_as_csv(columns, data)
            elif format_type == 'html':
                content = self._format_as_html(columns, data)
            else:
                content = self._format_as_table(columns, data)
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Exported results to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export results: {e}")
            return False


class QueryFormatter:
    """
    Formats SQL queries for better readability.
    """
    
    SQL_KEYWORDS = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 
        'INNER JOIN', 'OUTER JOIN', 'ON', 'GROUP BY', 'ORDER BY', 
        'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT',
        'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM',
        'CREATE TABLE', 'ALTER TABLE', 'DROP TABLE', 'AS', 'AND', 'OR'
    ]
    
    def format_sql(self, sql: str, indent_size: int = 2) -> str:
        """
        Format SQL query for better readability.
        
        Args:
            sql: SQL query string
            indent_size: Number of spaces for indentation
            
        Returns:
            Formatted SQL string
        """
        # Remove extra whitespace
        sql = ' '.join(sql.split())
        
        # Add newlines before major keywords
        for keyword in ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT']:
            sql = sql.replace(f' {keyword} ', f'\n{keyword} ')
            sql = sql.replace(f' {keyword.lower()} ', f'\n{keyword} ')
        
        # Handle JOIN clauses
        for join in ['LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN', 'JOIN']:
            sql = sql.replace(f' {join} ', f'\n{join} ')
            sql = sql.replace(f' {join.lower()} ', f'\n{join} ')
        
        # Indent subqueries
        lines = sql.split('\n')
        formatted_lines = []
        indent_level = 0
        
        for line in lines:
            line = line.strip()
            
            # Adjust indent for parentheses
            open_parens = line.count('(')
            close_parens = line.count(')')
            
            # Add indentation
            if indent_level > 0:
                line = ' ' * (indent_size * indent_level) + line
            
            formatted_lines.append(line)
            
            indent_level += open_parens - close_parens
            indent_level = max(0, indent_level)
        
        return '\n'.join(formatted_lines)
    
    def highlight_sql(self, sql: str) -> str:
        """
        Add simple highlighting to SQL keywords (for terminal display).
        
        Args:
            sql: SQL query string
            
        Returns:
            SQL with ANSI color codes for keywords
        """
        # ANSI color codes
        KEYWORD_COLOR = '\033[94m'  # Blue
        RESET_COLOR = '\033[0m'
        
        highlighted = sql
        
        for keyword in self.SQL_KEYWORDS:
            # Replace whole word only
            import re
            pattern = r'\b' + keyword + r'\b'
            highlighted = re.sub(
                pattern,
                f"{KEYWORD_COLOR}{keyword}{RESET_COLOR}",
                highlighted,
                flags=re.IGNORECASE
            )
        
        return highlighted


class ProgressFormatter:
    """
    Formats progress indicators and status messages.
    """
    
    def __init__(self):
        self.spinner_frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        self.current_frame = 0
    
    def get_spinner(self) -> str:
        """
        Get the next spinner frame.
        
        Returns:
            Spinner character
        """
        frame = self.spinner_frames[self.current_frame]
        self.current_frame = (self.current_frame + 1) % len(self.spinner_frames)
        return frame
    
    def format_progress_bar(self, current: int, total: int, width: int = 50) -> str:
        """
        Create a progress bar string.
        
        Args:
            current: Current progress value
            total: Total value
            width: Width of the progress bar
            
        Returns:
            Progress bar string
        """
        if total == 0:
            percent = 0
        else:
            percent = min(100, (current / total) * 100)
        
        filled = int(width * percent / 100)
        bar = '█' * filled + '░' * (width - filled)
        
        return f"[{bar}] {percent:.1f}% ({current}/{total})"
    
    def format_step(self, step_num: int, total_steps: int, description: str) -> str:
        """
        Format a step in a multi-step process.
        
        Args:
            step_num: Current step number
            total_steps: Total number of steps
            description: Step description
            
        Returns:
            Formatted step string
        """
        return f"Step [{step_num}/{total_steps}]: {description}"
    
    def format_duration(self, seconds: float) -> str:
        """
        Format a duration in seconds to human-readable format.
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Human-readable duration string
        """
        if seconds < 1:
            return f"{seconds*1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h {minutes}m"


# Singleton instances for convenience
result_formatter = ResultFormatter()
query_formatter = QueryFormatter()
progress_formatter = ProgressFormatter()