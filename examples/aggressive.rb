require 'hiredis'
require 'redis'
require 'securerandom'
require_relative '../lib/pesto.rb'

$key_num = 2
$concurrency = 3

def lock(ctx, pfx, pid = 0)
  pl = Pesto::Lock.new({ :redis => ctx[:redis], :verbose => true })
  kl = "pesto:#{pfx}"

  keys = []

  for i in 0..$key_num
    keys << "#{kl}:#{i}"
  end

  keys.shuffle!

  d1 = Time.now

  locked = pl.lockm(keys, { :timeout_lock => 0.005, :interval_check => 0.005 })

  if locked == 1
    puts "[#{pid}] lock acquired (took: #{(Time.now - d1) * 1000}ms)"
    pl.unlockm(keys)
  end
end

pfx = SecureRandom.hex(10)
children = []

def killall(pids)
  pids.each do |pid|
    Process.kill 9, pid
  end
  Process.exit
end

Signal.trap('INT')  { killall(children) }
Signal.trap('TERM')  { killall(children) }

for pid in 0..$concurrency
  puts "[#{pid}] fork"
  children << fork do
    redis = Redis.new
    while true do
      lock({ :redis => redis }, pfx, pid)
      delay = rand(1000).to_f / 10000.0
      sleep delay
    end
  end
end

while true do
  sleep 1
end
