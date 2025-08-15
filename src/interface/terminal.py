"""
Interactive terminal interface for the Text-to-SQL Agent.
Provides a user-friendly command-line interface for interacting with the agent.
"""

import os
import sys
import time
import readline  # For command history
from typing import Dict, Any, Optional, List
from datetime import datetime
from colorama import init, Fore, Back, Style
import logging

# Initialize colorama for cross-platform colored output
init(autoreset=True)

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import EnhancedTextToSQLAgent
from utils import result_formatter, query_formatter, progress_formatter
from .commands import CommandHandler

logger = logging.getLogger(__name__)


class InteractiveAgent:
    """
    Interactive terminal interface for the Text-to-SQL agent.
    
    This class provides a rich command-line experience with features like
    command history, auto-completion, and colored output.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the interactive agent.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or self._load_config()
        
        # Initialize the agent
        self.agent = None
        self.running = False
        
        # Command handler
        self.command_handler = CommandHandler(self)
        
        # Session information
        self.session_start = datetime.now()
        self.query_count = 0
        self.error_count = 0
        
        # Display settings
        self.colors_enabled = self.config.get('colors_enabled', True)
        self.verbose_mode = self.config.get('verbose_mode', False)
        self.auto_export = self.config.get('auto_export', False)
        
        # Command history
        self.history = []
        self.history_file = self.config.get('history_file', '.agent_history')
        self._load_history()
        
        # Auto-completion setup
        self._setup_autocomplete()
        
        logger.info("InteractiveAgent initialized")
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from environment or defaults.
        
        Returns:
            Configuration dictionary
        """
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        return {
            'api_key': os.getenv('OPENAI_API_KEY'),
            'db_path': os.getenv('DATABASE_PATH', 'data/ecommerce.db'),
            'model': os.getenv('MODEL_NAME', 'gpt-4'),
            'colors_enabled': os.getenv('COLORS_ENABLED', 'true').lower() == 'true',
            'verbose_mode': os.getenv('VERBOSE_MODE', 'false').lower() == 'true',
            'auto_export': os.getenv('AUTO_EXPORT', 'false').lower() == 'true',
            'history_file': os.getenv('HISTORY_FILE', '.agent_history'),
            'max_history': int(os.getenv('MAX_HISTORY', '100')),
            'enable_optimization': os.getenv('ENABLE_OPTIMIZATION', 'true').lower() == 'true',
            'enable_validation': os.getenv('ENABLE_VALIDATION', 'true').lower() == 'true'
        }
    
    def initialize_agent(self) -> bool:
        """
        Initialize the Text-to-SQL agent.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get API key
            api_key = self.config.get('api_key')
            if not api_key:
                api_key = self._prompt_api_key()
                if not api_key:
                    return False
            
            # Create agent
            self.agent = EnhancedTextToSQLAgent(
                api_key=api_key,
                db_path=self.config['db_path'],
                model=self.config['model'],
                enable_optimization=self.config['enable_optimization'],
                enable_validation=self.config['enable_validation']
            )
            
            logger.info("Agent initialized successfully")
            return True
            
        except Exception as e:
            self._print_error(f"Failed to initialize agent: {e}")
            logger.error(f"Agent initialization failed: {e}")
            return False
    
    def _prompt_api_key(self) -> Optional[str]:
        """
        Prompt user for OpenAI API key.
        
        Returns:
            API key or None
        """
        self._print_info("OpenAI API key not found in environment")
        api_key = input(f"{Fore.CYAN}Please enter your OpenAI API key: {Style.RESET_ALL}").strip()
        
        if api_key:
            # Optionally save to .env file
            save = input(f"{Fore.YELLOW}Save API key to .env file? (y/n): {Style.RESET_ALL}").lower()
            if save == 'y':
                with open('.env', 'a') as f:
                    f.write(f"\nOPENAI_API_KEY={api_key}\n")
                self._print_success("API key saved to .env file")
        
        return api_key if api_key else None
    
    def display_welcome(self):
        """Display welcome message and instructions."""
        self._clear_screen()
        
        # ASCII Art Banner
        banner = f"""
{Fore.CYAN}╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║  {Fore.WHITE}📊 TEXT-TO-SQL AGENT{Fore.CYAN}                                        ║
║  {Fore.YELLOW}Natural Language Database Interface{Fore.CYAN}                         ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝{Style.RESET_ALL}
        """
        
        print(banner)
        
        # System information
        print(f"\n{Fore.GREEN}System Information:{Style.RESET_ALL}")
        print(f"  • Database: {self.config['db_path']}")
        print(f"  • Model: {self.config['model']}")
        print(f"  • Optimization: {'✓' if self.config['enable_optimization'] else '✗'}")
        print(f"  • Validation: {'✓' if self.config['enable_validation'] else '✗'}")
        
        # Instructions
        print(f"\n{Fore.BLUE}Quick Start:{Style.RESET_ALL}")
        print("  • Type your question in natural language")
        print("  • Use 'help' to see all commands")
        print("  • Use Tab for auto-completion")
        print("  • Use ↑/↓ arrows for command history")
        print("  • Type 'exit' to quit")
        
        print(f"\n{Fore.MAGENTA}Example Questions:{Style.RESET_ALL}")
        examples = [
            "What are the top 5 best-selling products?",
            "Show me customers who spent more than $1000",
            "Which categories have the highest revenue?",
            "Find orders from the last 30 days"
        ]
        for example in examples:
            print(f"  • {example}")
        
        print("\n" + "="*60 + "\n")
    
    def run(self):
        """Main interaction loop."""
        # Initialize agent
        if not self.initialize_agent():
            self._print_error("Failed to initialize agent. Exiting.")
            return
        
        # Display welcome
        self.display_welcome()
        
        self.running = True
        
        while self.running:
            try:
                # Get user input with colored prompt
                prompt = self._get_prompt()
                user_input = input(prompt).strip()
                
                if not user_input:
                    continue
                
                # Add to history
                self.history.append(user_input)
                self._save_history()
                
                # Process input
                self._process_input(user_input)
                
            except KeyboardInterrupt:
                print("\n")
                if self._confirm("Do you want to exit?"):
                    self.running = False
                else:
                    print()  # New line for next prompt
                    
            except EOFError:
                # Ctrl+D pressed
                self.running = False
                
            except Exception as e:
                self._print_error(f"Unexpected error: {e}")
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        
        # Cleanup
        self._cleanup()
    
    def _get_prompt(self) -> str:
        """
        Get the input prompt string.
        
        Returns:
            Colored prompt string
        """
        if self.colors_enabled:
            return f"{Fore.GREEN}sql> {Style.RESET_ALL}"
        else:
            return "sql> "
    
    def _process_input(self, user_input: str):
        """
        Process user input.
        
        Args:
            user_input: User's input string
        """
        # Check if it's a command
        if self.command_handler.is_command(user_input):
            self.command_handler.execute_command(user_input)
        else:
            # Process as a natural language query
            self._process_query(user_input)
    
    def _process_query(self, query: str):
        """
        Process a natural language query.
        
        Args:
            query: Natural language query
        """
        self.query_count += 1
        
        # Show processing indicator
        print()
        self._print_info(f"Processing query #{self.query_count}...")
        
        # Animated processing
        if self.colors_enabled:
            spinner = progress_formatter.get_spinner()
            print(f"{spinner} Generating SQL...", end='\r')
        
        start_time = time.time()
        
        try:
            # Process the question
            result = self.agent.process_question(query)
            
            execution_time = time.time() - start_time
            
            if result['success']:
                self._display_results(result, execution_time)
                
                # Auto-export if enabled
                if self.auto_export and result.get('data'):
                    self._auto_export_results(result)
            else:
                self._print_error(f"Query failed: {result.get('error', 'Unknown error')}")
                self.error_count += 1
                
                # Show suggestions if available
                if 'validation_details' in result:
                    self._show_validation_feedback(result['validation_details'])
                    
        except Exception as e:
            self._print_error(f"Error processing query: {e}")
            self.error_count += 1
            logger.error(f"Query processing error: {e}", exc_info=True)
    
    def _display_results(self, result: Dict[str, Any], execution_time: float):
        """
        Display query results.
        
        Args:
            result: Query result dictionary
            execution_time: Total execution time
        """
        print("\n" + "="*60)
        
        # Display SQL query
        if self.verbose_mode or result.get('from_cache'):
            print(f"\n{Fore.BLUE}Generated SQL:{Style.RESET_ALL}")
            if self.colors_enabled:
                formatted_sql = query_formatter.highlight_sql(result['sql'])
            else:
                formatted_sql = query_formatter.format_sql(result['sql'])
            print(formatted_sql)
        
        # Display optimization info
        if 'optimization' in result and result['optimization']['is_optimized']:
            print(f"\n{Fore.YELLOW}Query Optimizations Applied:{Style.RESET_ALL}")
            for opt in result['optimization']['optimizations_applied']:
                print(f"  • {opt}")
        
        # Display explanation
        print(f"\n{Fore.GREEN}Explanation:{Style.RESET_ALL}")
        print(f"  {result['explanation']}")
        
        # Display results table
        print(f"\n{Fore.CYAN}Results ({result['row_count']} rows):{Style.RESET_ALL}")
        
        if result['data']:
            formatted_table = result_formatter.format_query_results(
                result['columns'],
                result['data'],
                format_type='grid',
                show_stats=self.verbose_mode
            )
            print(formatted_table)
        else:
            print("  No results found.")
        
        # Display performance metrics
        if self.verbose_mode:
            print(f"\n{Fore.MAGENTA}Performance Metrics:{Style.RESET_ALL}")
            print(f"  • SQL Generation: {result.get('generation_time', 0):.2f}s")
            print(f"  • Query Execution: {result.get('execution_time', 0):.2f}s")
            print(f"  • Total Time: {execution_time:.2f}s")
            
            if result.get('from_cache'):
                print(f"  • {Fore.GREEN}✓ Result from cache{Style.RESET_ALL}")
        
        print("="*60 + "\n")
    
    def _show_validation_feedback(self, validation_details: Dict[str, Any]):
        """
        Show validation feedback to user.
        
        Args:
            validation_details: Validation result details
        """
        if validation_details.get('suggestions'):
            print(f"\n{Fore.YELLOW}Suggestions:{Style.RESET_ALL}")
            for suggestion in validation_details['suggestions']:
                print(f"  • {suggestion}")
        
        if validation_details.get('warnings'):
            print(f"\n{Fore.YELLOW}Warnings:{Style.RESET_ALL}")
            for warning in validation_details['warnings']:
                print(f"  ⚠ {warning}")
    
    def _auto_export_results(self, result: Dict[str, Any]):
        """
        Automatically export results to file.
        
        Args:
            result: Query result dictionary
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"results_{timestamp}.csv"
        
        if result_formatter.export_to_file(
            result['columns'],
            result['data'],
            filename,
            format_type='csv'
        ):
            self._print_success(f"Results exported to {filename}")
    
    def _setup_autocomplete(self):
        """Set up tab auto-completion."""
        # Define completions
        self.completions = [
            'help', 'exit', 'quit', 'clear', 'history', 'stats',
            'schema', 'tables', 'export', 'config', 'verbose',
            'colors', 'reset', 'examples', 'optimize', 'validate',
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY',
            'What', 'Show', 'Find', 'List', 'Which', 'How many'
        ]
        
        def completer(text, state):
            options = [c for c in self.completions if c.lower().startswith(text.lower())]
            if state < len(options):
                return options[state]
            return None
        
        readline.set_completer(completer)
        readline.parse_and_bind('tab: complete')
    
    def _load_history(self):
        """Load command history from file."""
        try:
            if os.path.exists(self.history_file):
                readline.read_history_file(self.history_file)
        except Exception as e:
            logger.warning(f"Could not load history: {e}")
    
    def _save_history(self):
        """Save command history to file."""
        try:
            readline.write_history_file(self.history_file)
        except Exception as e:
            logger.warning(f"Could not save history: {e}")
    
    def _cleanup(self):
        """Perform cleanup operations."""
        print("\n" + "="*60)
        
        # Display session summary
        session_duration = datetime.now() - self.session_start
        
        print(f"\n{Fore.CYAN}Session Summary:{Style.RESET_ALL}")
        print(f"  • Duration: {session_duration}")
        print(f"  • Queries Processed: {self.query_count}")
        print(f"  • Errors Encountered: {self.error_count}")
        
        if self.agent:
            stats = self.agent.get_enhanced_statistics()
            print(f"  • Cache Hits: {stats.get('cache_size', 0)}")
        
        # Save history
        self._save_history()
        
        # Close agent connection
        if self.agent:
            self.agent.close()
        
        print(f"\n{Fore.GREEN}Thank you for using Text-to-SQL Agent!{Style.RESET_ALL}")
        print("="*60 + "\n")
    
    # Utility methods for colored output
    
    def _print_info(self, message: str):
        """Print info message."""
        if self.colors_enabled:
            print(f"{Fore.BLUE}ℹ {message}{Style.RESET_ALL}")
        else:
            print(f"INFO: {message}")
    
    def _print_success(self, message: str):
        """Print success message."""
        if self.colors_enabled:
            print(f"{Fore.GREEN}✓ {message}{Style.RESET_ALL}")
        else:
            print(f"SUCCESS: {message}")
    
    def _print_error(self, message: str):
        """Print error message."""
        if self.colors_enabled:
            print(f"{Fore.RED}✗ {message}{Style.RESET_ALL}")
        else:
            print(f"ERROR: {message}")
    
    def _print_warning(self, message: str):
        """Print warning message."""
        if self.colors_enabled:
            print(f"{Fore.YELLOW}⚠ {message}{Style.RESET_ALL}")
        else:
            print(f"WARNING: {message}")
    
    def _confirm(self, message: str) -> bool:
        """
        Get confirmation from user.
        
        Args:
            message: Confirmation message
            
        Returns:
            True if confirmed, False otherwise
        """
        response = input(f"{Fore.YELLOW}{message} (y/n): {Style.RESET_ALL}").lower()
        return response == 'y'
    
    def _clear_screen(self):
        """Clear the terminal screen."""
        os.system('clear' if os.name == 'posix' else 'cls')


def main():
    """Main entry point for the interactive agent."""
    try:
        agent = InteractiveAgent()
        agent.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()