require 'spec_helper'

describe Pesto::Lock do
  let(:rd) { double(:redis) }
  let(:pl) { Pesto::Lock.new redis: rd, verbose: true }

  describe "initialize" do
    it { expect(pl.class).to eq(Pesto::Lock) }
  end

  describe "merge options" do
    it { expect(pl.merge_options[:timeout_lock]).to eq(1) }
    it { expect(pl.merge_options(timeout_lock: 0.05)[:timeout_lock]).to eq(0.05) }
    it { expect(pl.merge_options({timeout_lock: 0.05}, [:invalid])).to eq({}) }
    it { expect(pl.merge_options({value1: 1, value2: 2}, :value1)).to eq({value1: 1}) }
  end

  describe "expire" do
    context "locking a single key" do
      let(:names) { [:a,:b] }
      before do
        expect(rd).to receive(:pipelined).and_return(1)
      end

      it { expect(pl.expire(names)).to eq(1) }
    end
  end

  describe "lock" do
    let(:redis) { Redis.new }
    let(:pli) { Pesto::Lock.new redis: redis, verbose: true }

    context "without conflicts" do
      before { redis.setnx("pesto:lock:not_working", 0)}
      it { expect(pli.lock("working")).to eq(1) }
    end

    context "with conflicts" do
      before { redis.setnx("pesto:lock:not_working", 1)}
      it { expect(pli.lock("not_working")).to eq(0) }
    end
  end

end
