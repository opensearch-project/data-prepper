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
License Header Compliance Checker for OpenSearch Data Prepper

This script checks that files contain the required license headers
as specified in CONTRIBUTING.md.

Usage:
  python check-license-headers.py file1.java file2.py ...
  echo "file1.java\nfile2.py" | python check-license-headers.py
"""

import os
import sys
from pathlib import Path
from typing import List

# File extensions that require license headers
SUPPORTED_EXTENSIONS = {
    '.java', '.groovy', '.gradle',  # Java ecosystem
    '.py',                          # Python
    '.sh', '.bash', '.zsh',        # Shell scripts
    '.yaml', '.yml',               # YAML files
    '.properties',                 # Properties files
}

def needs_license_header(file_path: str) -> bool:
    """Check if a file needs a license header based on its extension."""
    path = Path(file_path)
    return path.suffix.lower() in SUPPORTED_EXTENSIONS

def check_file_header(file_path: str) -> bool:
    """Check if a file has the required complete license header."""
    if not Path(file_path).exists():
        return True
        
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read first 15 lines to check for license header
            lines = []
            for i, line in enumerate(f):
                if i >= 15:  # Only check first 15 lines
                    break
                lines.append(line)
            
        content = ''.join(lines)
        
        # Check for all 5 required license header components
        required_components = [
            'Copyright OpenSearch Contributors',
            'SPDX-License-Identifier: Apache-2.0',
            'The OpenSearch Contributors require contributions made to',
            'this file be licensed under the Apache-2.0 license or a',
            'compatible open source license.'
        ]
        
        # All components must be present
        for component in required_components:
            if component not in content:
                return False
                
        return True
        
    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return True  # Skip files we can't read

def get_files_to_check() -> List[str]:
    """Get files to check from command line args or stdin."""
    if len(sys.argv) > 1:
        # Files provided as command line arguments
        return sys.argv[1:]
    else:
        # Read files from stdin
        files = []
        for line in sys.stdin:
            file_path = line.strip()
            if file_path:
                files.append(file_path)
        return files

def main():
    """Main function to check license headers."""
    files_to_check = get_files_to_check()
    
    if not files_to_check:
        print("No files to check", file=sys.stderr)
        return
    
    print(f"Checking {len(files_to_check)} files for license headers.")
    
    violations = []
    
    for file_path in files_to_check:
        print(f"Checking: {file_path}")
        
        if not Path(file_path).exists():
            print(f"  File not found: {file_path}")
            continue
            
        # Skip if doesn't need header
        if not needs_license_header(file_path):
            print(f"  Skipped (no header needed): {file_path}")
            continue
            
        # Check header
        if not check_file_header(file_path):
            violations.append(f"- `{file_path}`")
            print(f"  ❌ Missing license header: {file_path}")
        else:
            print(f"  ✅ Header OK: {file_path}")
    
    # Output results
    if violations:
        print(f"\n❌ Found {len(violations)} license header violations:")
        
        violation_text = '\n'.join(violations)
        
        # Set output for GitHub Actions
        github_output = os.environ.get('GITHUB_OUTPUT')
        if github_output:
            with open(github_output, 'a') as f:
                f.write(f"violations<<EOF\n{violation_text}\nEOF\n")
        
        print("\nViolations:")
        for violation in violations:
            print(f"  {violation}")
            
        sys.exit(1)
    else:
        print("\n✅ All files have proper license headers!")
        # Set empty output for GitHub Actions
        github_output = os.environ.get('GITHUB_OUTPUT')
        if github_output:
            with open(github_output, 'a') as f:
                f.write("violations=\n")

if __name__ == "__main__":
    main()