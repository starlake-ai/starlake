# encoding: utf-8

require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development, :test)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'rake'

require 'jeweler'
Jeweler::Tasks.new do |gem|
  # gem is a Gem::Specification... see http://docs.rubygems.org/read/chapter/20 for more options
  gem.name = "stopwords-filter"
  gem.homepage = "http://github.com/brenes/stopwords-filter"
  gem.license = "MIT"
  gem.summary = "Snowball based filters for stopwords"
  gem.description = "Small library that allows you to create a simple stopwords filter or use some based on Snowball stopwords lists"
  gem.email = "davidjbrenes@gmail.com"
  gem.authors = ["David J. Brenes"]
  # dependencies defined in Gemfile
end
Jeweler::RubygemsDotOrgTasks.new

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)

task :default => :spec
