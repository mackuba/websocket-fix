#!/usr/bin/env ruby

time_start = Time.now

require 'bundler/setup'
require 'skyfall'

module Skyfall
  class FastFirehose < Firehose
    def build_websocket_client(url)
      Faye::WebSocket::Client.new(url, nil, { headers: { 'User-Agent' => user_agent }, binary_data_format: :string })
    end

    def handle_message(msg)
      data = msg.data
      @handlers[:raw_message]&.call(data)

      if @handlers[:message]
        atp_message = Message.new(data)
        @cursor = atp_message.seq
        @handlers[:message].call(atp_message)
      else
        @cursor = nil
      end
    end
  end
end

if ARGV[1] == '-f'
  puts "Running in FAST mode:"
  sky = Skyfall::FastFirehose.new('bsky.network', :subscribe_repos, 1)
else
  puts "Running in normal mode:"
  sky = Skyfall::Firehose.new('bsky.network', :subscribe_repos, 1)
end

i = 0
target = ARGV[0].to_i

sky.on_raw_message do |msg|
  i += 1
  sky.disconnect if i == target
end

sky.on_connect do
  time_connected = Time.now
  puts "Connected (#{time_connected - time_start}s to connect)"
end

sky.connect
GC.start

puts "GC stats: #{GC.stat.inspect}"
