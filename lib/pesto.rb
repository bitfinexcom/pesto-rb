module Pesto
  class Lock

    def initialize(ctx = {}, opts = {})
      @ctx = ctx
      
      raise 'ERR_REDIS_NOTFOUND' if @ctx[:redis].nil?

      @conf = {
        :lock_expire => true,
        :timeout_lock_expire => 5,
        :timeout_lock => 1,
        :interval_check => 0.05
      }.merge(opts)
    end

    def rc
      @ctx[:redis]
    end

    def lock(_names, _opts = {})
      opts = {}.merge(
        @conf.select{ |k| [
          :timeout_lock_expire, :timeout_lock,
          :interval_check
        ].include?(k) 
        }
      ).merge(_opts || {})
     
      _names = [_names] if _names.is_a?(String)
      names = _names.uniq
      
      opts[:timeout_lock_expire] = (opts[:timeout_lock_expire] + (opts[:timeout_lock] * names.size)).to_i
               
      t_start = Time.now
      locked = 0

      while true 
        locked = 0

        lock_req = nil
        rc.with do |rc|
          lock_req = rc.pipelined do 
            names.each do |n|
              chash = lock_hash(n)
              rc.setnx chash, 1
            end
          end
        end

        locks = []

        if lock_req
          names.each_with_index do |n, ix|
            l = lock_req[ix]
            next if !l
            locked += 1
            locks << n
          end
        end

        if locked == names.size
          locked = 1

          if @conf[:lock_expire]
            rc.with do |rc|
              rc.pipelined do 
                names.each do |n|
                  chash = lock_hash(n)
                  rc.expire chash, opts[:timeout_lock_expire]
                end
              end
            end
          end
        else
          locked = 0
        end

        break if locked == 1 || (Time.now - t_start) > opts[:timeout_lock]

        unlock(locks)
        sleep opts[:interval_check]
      end

      locked == 0 ? 0 : 1
    end

    def locki(name = 'global', _opts = {})
      opts = (_opts || {}).merge({ :timeout_lock => 0 })
      lock(name, opts)
    end

    def lockx(name = 'global', opts = {}, err = 'ERR_LOCKING')
      locked = lock(name, opts)
      if locked == 1
        return 1
      end
      raise "#{err} (#{name})"
    end

    def lockm(_names = [], opts = {})
      lock(_names, opts)
    end

    def unlock(_names)
      _names = [_names] if _names.is_a?(String)
      names = _names.uniq

      unlock_req = nil
      rc.with do |rc|
        unlock_req = rc.pipelined do
          names.each do |n|
            rc.del(lock_hash(n))
          end
        end
      end

      val = 0
      
      if unlock_req
        unlock_req.each do |v| 
          val += v
        end
      end

      val > 0 ? 1 : 0
    end

    def unlockm(_names)
      unlock(_names)
    end

    private

    def lock_hash(name)
      "pesto:lock:#{name}"
    end

  end
end
