require File.expand_path('../lib/data_fabric/version', __FILE__)

Gem::Specification.new do |s|
  s.version = DataFabric::Version::STRING
  s.name = %q{data_fabric}
  s.authors = ["Mike Perham"]
  s.email = %q{mperham@gmail.com}
  s.extra_rdoc_files = [
    "README.rdoc"
  ]
  s.test_files = Dir.glob("test/**/*")
  s.files = Dir.glob("lib/**/*") + Dir.glob("example23/**/*") + Dir.glob("example30/**/*") + [
    "CHANGELOG",
    "README.rdoc",
    "Rakefile",
    "TESTING.rdoc"
  ]
  s.homepage = %q{http://github.com/mperham/data_fabric}
  s.require_paths = ["lib"]
  s.description = s.summary = %q{Sharding and replication support for ActiveRecord}
  s.add_development_dependency(%q<flexmock>, [">= 0"])
  s.add_development_dependency(%q<sqlite3>, [">= 0"])
end

