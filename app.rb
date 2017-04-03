require 'em/pure_ruby'
require 'appsignal'

module Statsd
  class Runner

    def self.default_config
      {
        :host           => "0.0.0.0",
        :port           => 8125,
        :environment    => 'production',
      }
    end

    def self.run!(opts = {})
      config = self.default_config.merge(opts)

      Appsignal.start_logger
      Appsignal.start

      EM::run do
        server = EM::open_datagram_socket(config[:host], config[:port], Server, config)
        puts "Now accepting connections on address #{config[:host]}, port #{config[:port]}..."
      end
    end
  end

  class Server < EM::Connection
    attr_reader :config

    def initialize(config)
      @config   = config
    end

    def tags_from_meta(str=nil)
      return {} unless str
      return {} unless str.start_with?('#')
      str[0] = ''  # Chop the '#'

      str.split(',').map do |tag_key_val|
        tag_key_val.split(':')
      end.to_h
    end

    def receive_data(msg)
      $stderr.puts msg if (@config[:debug])

      sample_rate = 1

      key_val, kind, meta = msg.split('|')
      key, val            = key_val.split(':')

      key  = key.gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.\$]/, '')
      val  = val || 1
      tags = tags_from_meta(meta)

      tags.each do |tag, val|
        key.gsub!("$#{tag}", val.downcase)
      end

      case kind.strip
      when 'ms'
        Appsignal.add_distribution_value(key, val.to_f)
      when 'c'
        /^@([\d\.]+)/.match(meta) {|m| sample_rate = m[1].to_f }
        Appsignal.increment_counter(key, ((val.to_f || 1) * (1/sample_rate)).to_i)
      when 'g'
        Appsignal.set_gauge(key, val.to_f)
      else
        # do nothing
        $stderr.puts "Unsupported type: #{msg}"
      end
    rescue => e
      $stderr.puts "Invalid line: #{msg} - #{e.inspect}"
    end

  end
end

Statsd::Runner.run!
