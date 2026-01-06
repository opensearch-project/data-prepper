#!/usr/bin/env python3

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

"""
Get newly added files from Git for license header checking.

This script identifies files added in the current PR and outputs them
one per line to stdout.
"""

import os
import subprocess
import sys

def get_newly_added_files():
    """Get list of files added in this PR."""
    try:
        # Get the base branch (usually main)
        base_ref = os.environ.get('GITHUB_BASE_REF', 'main')
        
        # Get added files in this PR
        result = subprocess.run([
            'git', 'diff', '--name-only', '--diff-filter=A', 
            f'origin/{base_ref}...HEAD'
        ], capture_output=True, text=True, check=True)
        
        files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
        return files
        
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e}", file=sys.stderr)
        return []

def main():
    """Main function to get newly added files."""
    files = get_newly_added_files()
    
    if not files:
        print("No newly added files found", file=sys.stderr)
        sys.exit(0)
    
    # Output files one per line
    for file_path in files:
        print(file_path)

if __name__ == "__main__":
    main()