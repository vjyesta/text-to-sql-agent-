#!/usr/bin/env python3
"""
Main entry point for the Text-to-SQL Agent.
Run this script to start the interactive terminal interface.
"""

import sys
import os

# Add the src directory to Python path
# This allows us to import our modules properly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.interface.terminal import InteractiveAgent


def main():
    """
    Main function to start the agent.
    """
    print("Starting Text-to-SQL Agent...")
    
    try:
        # Create and run the interactive agent
        agent = InteractiveAgent()
        agent.run()
    except KeyboardInterrupt:
        print("\n\nGoodbye!")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()