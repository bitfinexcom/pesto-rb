require 'connection_pool'
require 'redis'
require 'securerandom'
require_relative '../lib/pesto.rb'

$key_num = 5

def lock(ctx, pfx, pid = 0)
  pl = Pesto::Lock.new({ :pool => ctx[:pool], :verbose => true })
  keys = []

  for i in 0..$key_num
    keys << "pesto:#{pfx}:#{i}"
  end

  keys.shuffle!

  d1 = Time.now

  locked = pl.lock(keys, timeout_lock: 0.05, interval_check: 0.005 )

  if locked == 1
    pl.unlock(keys)
    puts "[#{pid}] lock acquired/dismissed (took: #{(Time.now - d1) * 1000}ms)"
  else
    puts "[#{pid}] lock failed"
  end
end

pfx = SecureRandom.hex(10)

def killall()
  Process.exit
end

Signal.trap('INT') { killall }
Signal.trap('TERM') { killall }

redis = ConnectionPool.new(size: 5, :timeout => 10) { Redis.new }
while true do
  lock({ :pool => redis }, pfx, 0)
end
