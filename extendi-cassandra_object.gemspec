# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = 'extendi-cassandra_object'
  s.version = '1.0.17'
  s.description = 'Cassandra ActiveModel'
  s.summary = 'Cassandra ActiveModel'
  s.authors = ['Duccio Giovannelli', 'gotime']
  s.email = 'giovannelli@extendi.it'
  s.homepage = 'https://github.com/giovannelli/cassandra_object'

  s.required_ruby_version     = '>= 1.9.2'
  s.required_rubygems_version = '>= 1.3.5'

  s.extra_rdoc_files = ['README.md']
  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test}/*`.split("\n")
  s.require_paths = ['lib']

  s.add_runtime_dependency('activemodel', '~> 5.2.0')
  s.add_runtime_dependency('cassandra-driver', '>= 3.2.3')
  s.add_runtime_dependency('lz4-ruby', '>= 0.3.3')

  s.platform = Gem::Platform::RUBY
  s.add_development_dependency('bundler')
end
