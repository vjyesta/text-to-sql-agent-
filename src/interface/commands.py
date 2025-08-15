"""
Command handler module for the terminal interface.
Manages all special commands and their execution.
"""

import os
import sys
import json
import csv
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from colorama import Fore, Style
import logging

logger = logging.getLogger(__name__)


class CommandHandler:
    """
    Handles special commands in the interactive interface.
    
    This class manages all non-query commands like help, export,
    configuration changes, etc.
    """
    
    def __init__(self, interface):
        """
        Initialize the command handler.
        
        Args:
            interface: Reference to the InteractiveAgent instance
        """
        self.interface = interface
        
        # Define available commands
        self.commands = {
            'help': {
                'func': self.cmd_help,
                'description': 'Show help information',
                'aliases': ['h', '?'],
                'usage': 'help [command]'
            },
            'exit': {
                'func': self.cmd_exit,
                'description': 'Exit the application',
                'aliases': ['quit', 'q'],
                'usage': 'exit'
            },
            'clear': {
                'func': self.cmd_clear,
                'description': 'Clear the screen',
                'aliases': ['cls'],
                'usage': 'clear'
            },
            'history': {
                'func': self.cmd_history,
                'description': 'Show command history',
                'aliases': ['hist'],
                'usage': 'history [n]'
            },
            'stats': {
                'func': self.cmd_stats,
                'description': 'Show session statistics',
                'aliases': ['statistics'],
                'usage': 'stats'
            },
            'schema': {
                'func': self.cmd_schema,
                'description': 'Show database schema',
                'aliases': ['structure'],
                'usage': 'schema [table]'
            },
            'tables': {
                'func': self.cmd_tables,
                'description': 'List all tables',
                'aliases': ['list'],
                'usage': 'tables'
            },
            'export': {
                'func': self.cmd_export,
                'description': 'Export last results',
                'aliases': ['save'],
                'usage': 'export [filename] [format]'
            },
            'config': {
                'func': self.cmd_config,
                'description': 'Show or set configuration',
                'aliases': ['settings'],
                'usage': 'config [key] [value]'
            },
            'verbose': {
                'func': self.cmd_verbose,
                'description': 'Toggle verbose mode',
                'aliases': ['v'],
                'usage': 'verbose'
            },
            'colors': {
                'func': self.cmd_colors,
                'description': 'Toggle colored output',
                'aliases': ['color'],
                'usage': 'colors'
            },
            'reset': {
                'func': self.cmd_reset,
                'description': 'Reset session and statistics',
                'aliases': ['restart'],
                'usage': 'reset'
            },
            'examples': {
                'func': self.cmd_examples,
                'description': 'Show example queries',
                'aliases': ['ex'],
                'usage': 'examples [category]'
            },
            'optimize': {
                'func': self.cmd_optimize,
                'description': 'Toggle query optimization',
                'aliases': ['opt'],
                'usage': 'optimize'
            },
            'validate': {
                'func': self.cmd_validate,
                'description': 'Toggle query validation',
                'aliases': ['val'],
                'usage': 'validate'
            },
            'cache': {
                'func': self.cmd_cache,
                'description': 'Manage query cache',
                'aliases': [],
                'usage': 'cache [clear|stats]'
            },
            'model': {
                'func': self.cmd_model,
                'description': 'Switch OpenAI model',
                'aliases': [],
                'usage': 'model [gpt-4|gpt-3.5-turbo]'
            },
            'debug': {
                'func': self.cmd_debug,
                'description': 'Toggle debug mode',
                'aliases': [],
                'usage': 'debug'
            },
            'test': {
                'func': self.cmd_test,
                'description': 'Run test queries',
                'aliases': [],
                'usage': 'test [suite]'
            },
            'analyze': {
                'func': self.cmd_analyze,
                'description': 'Analyze a SQL query',
                'aliases': [],
                'usage': 'analyze <sql_query>'
            },
            'compare': {
                'func': self.cmd_compare,
                'description': 'Compare two queries',
                'aliases': [],
                'usage': 'compare'
            },
            'benchmark': {
                'func': self.cmd_benchmark,
                'description': 'Run performance benchmark',
                'aliases': [],
                'usage': 'benchmark'
            }
        }
        
        # Build alias mapping
        self.aliases = {}
        for cmd, info in self.commands.items():
            for alias in info.get('aliases', []):
                self.aliases[alias] = cmd
        
        # Store last result for export
        self.last_result = None
        
        logger.info("CommandHandler initialized with %d commands", len(self.commands))
    
    def is_command(self, input_text: str) -> bool:
        """
        Check if input is a command.
        
        Args:
            input_text: User input
            
        Returns:
            True if input is a command
        """
        if not input_text:
            return False
        
        first_word = input_text.split()[0].lower()
        return first_word in self.commands or first_word in self.aliases
    
    def execute_command(self, input_text: str) -> bool:
        """
        Execute a command.
        
        Args:
            input_text: User input
            
        Returns:
            True if command was executed successfully
        """
        parts = input_text.split(maxsplit=1)
        command = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""
        
        # Resolve alias
        if command in self.aliases:
            command = self.aliases[command]
        
        # Execute command
        if command in self.commands:
            try:
                self.commands[command]['func'](args)
                return True
            except Exception as e:
                self.interface._print_error(f"Command failed: {e}")
                logger.error(f"Command execution failed: {e}", exc_info=True)
                return False
        
        return False
    
    # Command implementations
    
    def cmd_help(self, args: str):
        """Show help information."""
        if args:
            # Show help for specific command
            cmd = args.strip().lower()
            if cmd in self.aliases:
                cmd = self.aliases[cmd]
            
            if cmd in self.commands:
                info = self.commands[cmd]
                print(f"\n{Fore.CYAN}Command: {cmd}{Style.RESET_ALL}")
                print(f"  Description: {info['description']}")
                print(f"  Usage: {info['usage']}")
                if info['aliases']:
                    print(f"  Aliases: {', '.join(info['aliases'])}")
            else:
                self.interface._print_error(f"Unknown command: {cmd}")
        else:
            # Show all commands
            print(f"\n{Fore.CYAN}Available Commands:{Style.RESET_ALL}")
            print("="*60)
            
            # Group commands by category
            categories = {
                'Basic': ['help', 'exit', 'clear', 'history'],
                'Database': ['schema', 'tables', 'analyze'],
                'Results': ['export', 'cache', 'compare'],
                'Settings': ['config', 'verbose', 'colors', 'model'],
                'Advanced': ['optimize', 'validate', 'debug', 'benchmark'],
                'Other': ['stats', 'reset', 'examples', 'test']
            }
            
            for category, cmds in categories.items():
                print(f"\n{Fore.YELLOW}{category}:{Style.RESET_ALL}")
                for cmd in cmds:
                    if cmd in self.commands:
                        info = self.commands[cmd]
                        aliases = f" ({', '.join(info['aliases'])})" if info['aliases'] else ""
                        print(f"  {cmd:15} - {info['description']}{aliases}")
            
            print(f"\n{Fore.GREEN}Tips:{Style.RESET_ALL}")
            print("  • Type 'help <command>' for detailed help")
            print("  • Use Tab for auto-completion")
            print("  • Use ↑/↓ arrows for command history")
            print("  • Natural language queries don't need commands")
    
    def cmd_exit(self, args: str):
        """Exit the application."""
        if self.interface._confirm("Are you sure you want to exit?"):
            self.interface.running = False
    
    def cmd_clear(self, args: str):
        """Clear the screen."""
        self.interface._clear_screen()
        self.interface.display_welcome()
    
    def cmd_history(self, args: str):
        """Show command history."""
        n = 20  # Default number of items
        if args:
            try:
                n = int(args)
            except ValueError:
                self.interface._print_error("Invalid number")
                return
        
        print(f"\n{Fore.CYAN}Command History (last {n}):{Style.RESET_ALL}")
        print("="*60)
        
        history = self.interface.history[-n:] if len(self.interface.history) > n else self.interface.history
        
        for i, cmd in enumerate(history, 1):
            timestamp = datetime.now().strftime("%H:%M:%S")  # Simplified
            print(f"  {i:3}. [{timestamp}] {cmd}")
    
    def cmd_stats(self, args: str):
        """Show session statistics."""
        print(f"\n{Fore.CYAN}Session Statistics:{Style.RESET_ALL}")
        print("="*60)
        
        # Session info
        duration = datetime.now() - self.interface.session_start
        print(f"\n{Fore.YELLOW}Session:{Style.RESET_ALL}")
        print(f"  • Start Time: {self.interface.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  • Duration: {duration}")
        print(f"  • Queries Processed: {self.interface.query_count}")
        print(f"  • Errors: {self.interface.error_count}")
        
        # Agent statistics
        if self.interface.agent:
            stats = self.interface.agent.get_enhanced_statistics()
            
            print(f"\n{Fore.YELLOW}Cache Performance:{Style.RESET_ALL}")
            print(f"  • Cache Size: {stats.get('cache_size', 0)}")
            print(f"  • Total Queries: {stats.get('total_queries', 0)}")
            
            print(f"\n{Fore.YELLOW}Optimization:{Style.RESET_ALL}")
            print(f"  • Queries Optimized: {stats.get('queries_optimized', 0)}")
            print(f"  • Avg Improvement: {stats.get('average_optimization_improvement', 0):.1f}%")
            
            print(f"\n{Fore.YELLOW}Validation:{Style.RESET_ALL}")
            print(f"  • Queries Validated: {stats.get('queries_validated', 0)}")
            print(f"  • Validation Failures: {stats.get('validation_failures', 0)}")
    
    def cmd_schema(self, args: str):
        """Show database schema."""
        if not self.interface.agent:
            self.interface._print_error("Agent not initialized")
            return
        
        if args:
            # Show specific table schema
            table_name = args.strip()
            self._show_table_schema(table_name)
        else:
            # Show all tables
            print(f"\n{Fore.CYAN}Database Schema:{Style.RESET_ALL}")
            print("="*60)
            print(self.interface.agent.schema)
    
    def cmd_tables(self, args: str):
        """List all tables."""
        if not self.interface.agent:
            self.interface._print_error("Agent not initialized")
            return
        
        try:
            cursor = self.interface.agent.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            print(f"\n{Fore.CYAN}Database Tables:{Style.RESET_ALL}")
            print("="*60)
            
            for i, (table,) in enumerate(tables, 1):
                if not table.startswith('sqlite_'):
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  {i:2}. {table:30} ({count:,} rows)")
                    
        except Exception as e:
            self.interface._print_error(f"Failed to list tables: {e}")
    
    def cmd_export(self, args: str):
        """Export last results."""
        if not self.last_result:
            self.interface._print_error("No results to export")
            return
        
        # Parse arguments
        parts = args.split()
        filename = parts[0] if parts else f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        format_type = parts[1] if len(parts) > 1 else 'csv'
        
        # Add extension if not present
        extensions = {'csv': '.csv', 'json': '.json', 'html': '.html'}
        if format_type in extensions and not filename.endswith(extensions[format_type]):
            filename += extensions[format_type]
        
        # Export
        from utils import result_formatter
        
        if result_formatter.export_to_file(
            self.last_result['columns'],
            self.last_result['data'],
            filename,
            format_type
        ):
            self.interface._print_success(f"Results exported to {filename}")
        else:
            self.interface._print_error("Export failed")
    
    def cmd_config(self, args: str):
        """Show or set configuration."""
        parts = args.split(maxsplit=1)
        
        if not parts:
            # Show all config
            print(f"\n{Fore.CYAN}Configuration:{Style.RESET_ALL}")
            print("="*60)
            
            for key, value in self.interface.config.items():
                if key != 'api_key':  # Don't show API key
                    print(f"  {key:20} = {value}")
        elif len(parts) == 1:
            # Show specific config
            key = parts[0]
            if key in self.interface.config:
                print(f"{key} = {self.interface.config[key]}")
            else:
                self.interface._print_error(f"Unknown config key: {key}")
        else:
            # Set config
            key, value = parts[0], parts[1]
            
            # Convert value to appropriate type
            if value.lower() in ['true', 'false']:
                value = value.lower() == 'true'
            elif value.isdigit():
                value = int(value)
            
            self.interface.config[key] = value
            self.interface._print_success(f"Set {key} = {value}")
    
    def cmd_verbose(self, args: str):
        """Toggle verbose mode."""
        self.interface.verbose_mode = not self.interface.verbose_mode
        status = "ON" if self.interface.verbose_mode else "OFF"
        self.interface._print_success(f"Verbose mode: {status}")
    
    def cmd_colors(self, args: str):
        """Toggle colored output."""
        self.interface.colors_enabled = not self.interface.colors_enabled
        status = "ON" if self.interface.colors_enabled else "OFF"
        self.interface._print_success(f"Colored output: {status}")
    
    def cmd_reset(self, args: str):
        """Reset session and statistics."""
        if self.interface._confirm("Reset all statistics and clear cache?"):
            if self.interface.agent:
                self.interface.agent.reset_statistics()
                self.interface.agent.clear_cache()
            
            self.interface.query_count = 0
            self.interface.error_count = 0
            self.interface.session_start = datetime.now()
            
            self.interface._print_success("Session reset complete")
    
    def cmd_examples(self, args: str):
        """Show example queries."""
        categories = {
            'basic': [
                "Show all products",
                "List customers from New York",
                "Find orders from last week"
            ],
            'aggregation': [
                "What is the total revenue?",
                "How many orders per month?",
                "Average order value by customer type"
            ],
            'analysis': [
                "Top 10 best-selling products",
                "Customers who spent more than $1000",
                "Products with low stock"
            ],
            'complex': [
                "Which products are frequently bought together?",
                "Customer lifetime value analysis",
                "Revenue trend over time"
            ]
        }
        
        if args and args in categories:
            # Show specific category
            print(f"\n{Fore.CYAN}{args.title()} Examples:{Style.RESET_ALL}")
            for example in categories[args]:
                print(f"  • {example}")
        else:
            # Show all categories
            print(f"\n{Fore.CYAN}Example Queries:{Style.RESET_ALL}")
            print("="*60)
            
            for category, examples in categories.items():
                print(f"\n{Fore.YELLOW}{category.title()}:{Style.RESET_ALL}")
                for example in examples:
                    print(f"  • {example}")
    
    def cmd_optimize(self, args: str):
        """Toggle query optimization."""
        if self.interface.agent:
            self.interface.agent.enable_optimization = not self.interface.agent.enable_optimization
            status = "ON" if self.interface.agent.enable_optimization else "OFF"
            self.interface._print_success(f"Query optimization: {status}")
        else:
            self.interface._print_error("Agent not initialized")
    
    def cmd_validate(self, args: str):
        """Toggle query validation."""
        if self.interface.agent:
            self.interface.agent.enable_validation = not self.interface.agent.enable_validation
            status = "ON" if self.interface.agent.enable_validation else "OFF"
            self.interface._print_success(f"Query validation: {status}")
        else:
            self.interface._print_error("Agent not initialized")
    
    def cmd_cache(self, args: str):
        """Manage query cache."""
        if not self.interface.agent:
            self.interface._print_error("Agent not initialized")
            return
        
        if args == 'clear':
            self.interface.agent.clear_cache()
            self.interface._print_success("Cache cleared")
        elif args == 'stats':
            stats = self.interface.agent.get_statistics()
            print(f"\n{Fore.CYAN}Cache Statistics:{Style.RESET_ALL}")
            print(f"  • Cache Size: {stats.get('cache_size', 0)}")
            print(f"  • Hit Rate: {stats.get('cache_hit_rate', 0):.1f}%")
        else:
            self.interface._print_info("Usage: cache [clear|stats]")
    
    def cmd_model(self, args: str):
        """Switch OpenAI model."""
        valid_models = ['gpt-4', 'gpt-3.5-turbo', 'gpt-4-turbo']
        
        if args in valid_models:
            if self.interface.agent:
                self.interface.agent.model = args
                self.interface._print_success(f"Switched to model: {args}")
            else:
                self.interface.config['model'] = args
                self.interface._print_success(f"Model will be set to {args} on next init")
        else:
            self.interface._print_error(f"Invalid model. Choose from: {', '.join(valid_models)}")
    
    def cmd_debug(self, args: str):
        """Toggle debug mode."""
        current_level = logger.getEffectiveLevel()
        if current_level == logging.DEBUG:
            logging.getLogger().setLevel(logging.INFO)
            self.interface._print_success("Debug mode: OFF")
        else:
            logging.getLogger().setLevel(logging.DEBUG)
            self.interface._print_success("Debug mode: ON")
    
    def cmd_test(self, args: str):
        """Run test queries."""
        test_queries = [
            "Show top 5 products",
            "Total revenue by month",
            "Customers with no orders"
        ]
        
        print(f"\n{Fore.CYAN}Running Test Suite:{Style.RESET_ALL}")
        print("="*60)
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nTest {i}: {query}")
            try:
                result = self.interface.agent.process_question(query)
                if result['success']:
                    print(f"  {Fore.GREEN}✓ Passed{Style.RESET_ALL} ({result['row_count']} rows)")
                else:
                    print(f"  {Fore.RED}✗ Failed{Style.RESET_ALL}: {result.get('error')}")
            except Exception as e:
                print(f"  {Fore.RED}✗ Error{Style.RESET_ALL}: {e}")
    
    def cmd_analyze(self, args: str):
        """Analyze a SQL query."""
        if not args:
            self.interface._print_error("Please provide a SQL query to analyze")
            return
        
        if not self.interface.agent:
            self.interface._print_error("Agent not initialized")
            return
        
        print(f"\n{Fore.CYAN}Query Analysis:{Style.RESET_ALL}")
        print("="*60)
        
        # Validate query
        if hasattr(self.interface.agent, 'validator'):
            validation = self.interface.agent.validator.validate(args)
            
            print(f"\n{Fore.YELLOW}Validation:{Style.RESET_ALL}")
            print(f"  • Valid: {'✓' if validation['is_valid'] else '✗'}")
            print(f"  • Risk Level: {validation.get('risk_level', 'unknown')}")
            
            if validation.get('warnings'):
                print(f"\n{Fore.YELLOW}Warnings:{Style.RESET_ALL}")
                for warning in validation['warnings']:
                    print(f"  • {warning}")
        
        # Optimize query
        if hasattr(self.interface.agent, 'optimizer'):
            optimization = self.interface.agent.optimizer.optimize(args)
            
            if optimization['is_optimized']:
                print(f"\n{Fore.YELLOW}Optimizations:{Style.RESET_ALL}")
                for opt in optimization['optimizations_applied']:
                    print(f"  • {opt}")
                print(f"\n{Fore.GREEN}Optimized Query:{Style.RESET_ALL}")
                print(optimization['optimized_query'])
    
    def cmd_compare(self, args: str):
        """Compare two queries."""
        print(f"\n{Fore.CYAN}Query Comparison Mode{Style.RESET_ALL}")
        print("Enter two queries to compare (empty line to finish each):")
        
        # Get first query
        print(f"\n{Fore.YELLOW}Query 1:{Style.RESET_ALL}")
        query1 = input().strip()
        
        # Get second query
        print(f"\n{Fore.YELLOW}Query 2:{Style.RESET_ALL}")
        query2 = input().strip()
        
        if not query1 or not query2:
            self.interface._print_error("Both queries are required")
            return
        
        # Process both queries
        print(f"\n{Fore.CYAN}Comparing Queries:{Style.RESET_ALL}")
        print("="*60)
        
        try:
            result1 = self.interface.agent.process_question(query1)
            result2 = self.interface.agent.process_question(query2)
            
            # Compare results
            print(f"\n{Fore.YELLOW}Query 1 Results:{Style.RESET_ALL}")
            print(f"  • Rows: {result1.get('row_count', 0)}")
            print(f"  • Time: {result1.get('execution_time', 0):.2f}s")
            
            print(f"\n{Fore.YELLOW}Query 2 Results:{Style.RESET_ALL}")
            print(f"  • Rows: {result2.get('row_count', 0)}")
            print(f"  • Time: {result2.get('execution_time', 0):.2f}s")
            
            # Show which is faster
            if result1.get('execution_time', 0) < result2.get('execution_time', 0):
                print(f"\n{Fore.GREEN}Query 1 is faster{Style.RESET_ALL}")
            else:
                print(f"\n{Fore.GREEN}Query 2 is faster{Style.RESET_ALL}")
                
        except Exception as e:
            self.interface._print_error(f"Comparison failed: {e}")
    
    def cmd_benchmark(self, args: str):
        """Run performance benchmark."""
        print(f"\n{Fore.CYAN}Running Performance Benchmark:{Style.RESET_ALL}")
        print("="*60)
        
        benchmark_queries = [
            "SELECT COUNT(*) FROM orders",
            "SELECT * FROM products LIMIT 10",
            "SELECT c.first_name, COUNT(o.order_id) FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id",
        ]
        
        total_time = 0
        
        for i, query in enumerate(benchmark_queries, 1):
            print(f"\nBenchmark {i}: {query[:50]}...")
            
            try:
                import time
                start = time.time()
                
                cursor = self.interface.agent.conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                
                elapsed = time.time() - start
                total_time += elapsed
                
                print(f"  • Time: {elapsed:.3f}s")
                print(f"  • Rows: {len(results)}")
                
            except Exception as e:
                print(f"  • {Fore.RED}Failed{Style.RESET_ALL}: {e}")
        
        print(f"\n{Fore.GREEN}Total Time: {total_time:.3f}s{Style.RESET_ALL}")
    
    def _show_table_schema(self, table_name: str):
        """Show schema for a specific table."""
        try:
            cursor = self.interface.agent.conn.cursor()
            
            # Get table info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            if not columns:
                self.interface._print_error(f"Table '{table_name}' not found")
                return
            
            print(f"\n{Fore.CYAN}Table: {table_name}{Style.RESET_ALL}")
            print("="*60)
            
            print(f"\n{Fore.YELLOW}Columns:{Style.RESET_ALL}")
            for col in columns:
                cid, name, dtype, notnull, default, pk = col
                nullable = "NOT NULL" if notnull else "NULL"
                primary = " [PRIMARY KEY]" if pk else ""
                default_val = f" DEFAULT {default}" if default else ""
                print(f"  • {name}: {dtype} {nullable}{primary}{default_val}")
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            if foreign_keys:
                print(f"\n{Fore.YELLOW}Foreign Keys:{Style.RESET_ALL}")
                for fk in foreign_keys:
                    print(f"  • {fk[3]} → {fk[2]}.{fk[4]}")
            
            # Get indexes
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            
            if indexes:
                print(f"\n{Fore.YELLOW}Indexes:{Style.RESET_ALL}")
                for idx in indexes:
                    print(f"  • {idx[1]}")
                    
        except Exception as e:
            self.interface._print_error(f"Failed to get schema: {e}")
    
    def store_result(self, result: Dict[str, Any]):
        """
        Store result for export.
        
        Args:
            result: Query result to store
        """
        self.last_result = result