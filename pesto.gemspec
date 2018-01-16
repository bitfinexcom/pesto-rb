lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pesto/version'

Gem::Specification.new do |s|
  s.name        = 'pesto'
  s.version     = Pesto::VERSION
  s.summary     = 'dlock'
  s.description     = 'distributed locking with deadlock prevention'
  s.authors     = ['bfx devs']
  s.email       = 'info@bitfinex.com'
  s.homepage    = 'https://www.bitfinex.com'
  s.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  s.require_paths = ["lib"]

  s.add_runtime_dependency "hiredis", "~> 0.6"
  s.add_runtime_dependency "redis", "~> 4.0"
  s.add_runtime_dependency "connection_pool", "~> 2.2"

  s.add_development_dependency "rspec", "~> 3.6"
  s.add_development_dependency "fakeredis", "~> 0.1.4"
  s.add_development_dependency "simplecov", "~> 0.14"
end
