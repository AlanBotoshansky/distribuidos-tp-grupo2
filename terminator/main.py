#!/usr/bin/env python3

import argparse
import docker
from src.terminator import Terminator

DEFAULT_PROJECT_NAME = 'tp'

def main():
    parser = argparse.ArgumentParser(
        description="Docker Container Terminator - Kill containers from docker-compose project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Kill specific containers or all containers in specific clusters:
  python3 main.py --mode specific --arg "movies_filter_released_between_2000_and_2009_1, movies_filter_by_one_production_country"

  # Kill N random containers:
  python3 main.py --mode count --arg 3

  # Kill all containers:
  python3 main.py --mode all

  # Kill all containers except specific ones or entire clusters:
  python3 main.py --mode all --arg "data_cleaner,results_handler,movies_filter_by_one_production_country"
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['specific', 'count', 'all'],
        required=True,
        help='Termination mode: specific (kill specific containers), count (kill N containers), all (kill all containers)'
    )
    
    parser.add_argument(
        '--arg',
        help='Argument for the mode: comma-separated container names for "specific", number for "count", comma-separated excluded containers for "all" (optional)'
    )
    
    parser.add_argument(
        '--project-name',
        default=DEFAULT_PROJECT_NAME,
        help=f'Docker Compose project name (default: {DEFAULT_PROJECT_NAME})'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'specific' and not args.arg:
        parser.error("--arg is required for 'specific' mode (comma-separated container names)")
    elif args.mode == 'count' and not args.arg:
        parser.error("--arg is required for 'count' mode (number of containers to kill)")
    elif args.mode == 'count' and args.arg:
        try:
            int(args.arg)
        except ValueError:
            parser.error("--arg must be a number for 'count' mode")
    
    terminator = Terminator(args.project_name)
    
    try:
        if args.mode == 'specific':
            container_names = {name.strip() for name in args.arg.split(',') if name.strip()}
            terminator.kill_specific_containers(container_names)
        elif args.mode == 'count':
            count = int(args.arg)
            terminator.kill_count(count)
        elif args.mode == 'all':
            excluded_container_names = set()
            if args.arg:
                excluded_container_names = {name.strip() for name in args.arg.split(',') if name.strip()}
            terminator.kill_all(excluded_container_names)
    except docker.errors.APIError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
