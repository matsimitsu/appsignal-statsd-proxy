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

    def receive_data(msg)
      $stderr.puts msg if (@config[:debug])

      bits = msg.split(':')
      key  = bits.first.gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.]/, '')

      bits << '1' if bits.empty?

      bits.each do |b|
        next unless b.include? '|'

        sample_rate = 1
        fields      = b.split('|')

        if fields[1]
          case fields[1].strip
          when 'ms'
            Appsignal.add_distribution_value(key, fields[0].to_f)
          when 'c'
            /^@([\d\.]+)/.match(fields[2]) {|m| sample_rate = m[1].to_f }
            Appsignal.increment_counter(key, ((fields[0].to_f || 1) * (1/sample_rate)).to_i)
          when 'g'
            Appsignal.set_gauge(key, fields[0].to_f)
          else
            # do nothing
            $stderr.puts "Unsupported type: #{msg}"
          end
        else
          $stderr.puts "Invalid line: #{msg}"
        end
      end
    end

  end
end

Statsd::Runner.run!
