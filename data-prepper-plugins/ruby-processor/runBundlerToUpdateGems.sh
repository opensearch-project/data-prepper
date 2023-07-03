#!/bin/bash

jruby_jar_path=$1
echo "Starting runBundlerToUpdateGems.sh...."
# Check if JRuby jar path is provided
if [[ -z "$jruby_jar_path" ]]; then
  echo "Please provide the JRuby jar path as an argument."
  exit 1
fi

# Extract JRuby version from the jar path
jar_version=$(echo $jruby_jar_path | grep -o -E 'jruby-complete-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)' | sed 's/jruby-complete-//g')

# Read the line containing the ruby and jruby versions from Gemfile
version_line=$(grep "ruby '" Gemfile)

# Extract the ruby version
ruby_version=$(echo $version_line | awk -F"'" '{print $2}')

# Extract the jruby version
gemfile_version=$(echo $version_line | awk -F"=> " '{print $3}' | tr -d "'")

# Print the versions (for debugging purposes)
echo "Ruby version: $ruby_version"
echo "JRuby version: $gemfile_version"

# Compare versions and exit if they don't match
if [[ $jar_version != $gemfile_version ]]; then
  echo "The JRuby version in the Gemfile ($gemfile_version) does not match the JRuby version in the jar ($jar_version)."
  exit 1
fi

# Assert that `ruby $RUBY_VERSION` is specified in the Gemfile
if ! grep -q "ruby '$ruby_version'" Gemfile; then
  echo "The Ruby version in the Gemfile does not match the Ruby version in the JRuby jar ($ruby_version)."
  exit 1
fi

# remove all files in the rubygems directory
rm -rf rubygems/*

# Set bundle local path
java -jar $jruby_jar_path -S bundle config set --local path 'rubygems'

# Run bundle install

java -jar $jruby_jar_path -S bundle install

# Move rubygems directory
mv rubygems/jruby/$ruby_version/* rubygems/

# Create jar file without changing the directory
jar -cf rubygems/rubygems.jar -C rubygems .

# Clean up the rubygems directory, preserving the rubygems.jar
find rubygems/* ! -name 'rubygems.jar' -type f -exec rm -f {} +
find rubygems/* ! -name 'rubygems.jar' -type d -exec rm -rf {} +

echo "Finished installing gems."