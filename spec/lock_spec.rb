require 'spec_helper'

describe Pesto::Lock do
  let(:pl) {Pesto::Lock.new redis: Redis.new(driver: :hiredis), verbose: true}

  describe "initialize" do
    it { expect(pl.class).to eq(Pesto::Lock) }
  end

  describe "merge options" do
    it { expect(pl.merge_options[:timeout_lock]).to eq(90) }
    it { expect(pl.merge_options(timeout_lock: 0.05)[:timeout_lock]).to eq(0.05) }
    it { expect(pl.merge_options({timeout_lock: 0.05}, [:invalid])).to eq({}) }
    it { expect(pl.merge_options({value1: 1, value2: 2}, :value1)).to eq({value1: 1}) }
  end

  describe "lock" do
    it {expect(pl.lock
  end

end
