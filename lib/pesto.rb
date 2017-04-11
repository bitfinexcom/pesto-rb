module Pesto
  class Lock

    def initialize(ctx = {}, opts = {})
      @ctx = ctx
      
      raise 'ERR_REDIS_NOTFOUND' if @ctx[:redis].nil?

      @conf = {
        :timeout_concurrency_expire => 60,
        :timeout_lock_expire => 300,
        :timeout_lock => 90,
        :interval_check => 0.1,
        :concurrency_limit => 3
      }.merge(opts)

    end

    def get_concurrency(name)
      hash = ccc_hash(name) 
      res = @ctx[:redis].get(hash) || 0
      res.to_i
    end

    def incr_concurrency(name)
      hash = ccc_hash(name) 
      @ctx[:redis].incr(hash)
      @ctx[:redis].expire(hash, @conf[:timeout_concurrency_expire])
    end

    def decr_concurrency(name)
      hash = ccc_hash(name) 
      @ctx[:redis].decr(hash)
      @ctx[:redis].expire(hash, @conf[:timeout_concurrency_expire])
    end

    def lock(name = 'global', _opts = {})
      opts = {}.merge(
        @conf.select{ |k| [
          :timeout_lock_expire, :timeout_lock, :interval_check, :concurrency_limit
        ].include?(k) 
        }
      ).merge(_opts || {})

      if opts[:concurrency_limit] > 0
        ccc = get_concurrency(name)
        return 0 if ccc > opts[:concurrency_limit]
      end

      incr_concurrency(name)

      chash = lock_hash(name)
      locked_old = 1

      t_start = Time.now

      while locked_old
        locked_old = @ctx[:redis].get(chash) 

        if locked_old.nil?
          is_set = @ctx[:redis].setnx chash, 1
          if is_set
            @ctx[:redis].expire chash, opts[:timeout_lock_expire]
            locked_old = nil
            break
          else
            locked_old = 1
          end
        else
          locked_old = 1
        end

        break if (Time.now - t_start) > opts[:timeout_lock]
        sleep opts[:interval_check]
      end

      decr_concurrency(name)

      is_locked = locked_old == 1 ? 0 : 1
      is_locked
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
      names = _names.uniq
      opts[:timeout_lock_expire] = opts[:timeout_lock_expire] || 300
      opts[:timeout_lock] = opts[:timeout_lock] || 65
      opts[:timeout_lock_expire] = (opts[:timeout_lock_expire] + (opts[:timeout_lock] * names.size)).to_i

      locks = []
      valid = true

      names.each do |n|
        l = lock(n, opts)
        if l != 1
          valid = false
          break
        end
        locks << n
      end

      if !valid
        unlockm(locks)
      end

      return valid ? 1 : 0
    end

    def unlock(name)
      @ctx[:redis].del lock_hash(name)
    end

    def unlockm(_names)
      names = _names.uniq
      val = 0
      names.each do |n|
        val += unlock(n)
      end
      val > 0 ? 1 : 0
    end

    private

    def ccc_hash(name)
      "pesto:concurrency:#{name}"
    end

    def lock_hash(name)
      "pesto:lock:#{name}"
    end

  end
end
