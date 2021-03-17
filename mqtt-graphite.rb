#!/usr/bin/ruby
require 'mqtt'
require 'logger'
require 'yaml'
require 'ostruct'
require 'json'
require 'time'
require 'socket'

def usage
  puts "usage : #{$0} config.yaml"
  exit 1
end

$stdout.sync = true
Dir.chdir(File.dirname($0))
$current_dir = Dir.pwd

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

usage if ARGV.size == 0

$conf = OpenStruct.new(YAML.load_file(ARGV[0]))

$conn_opts = {
  remote_host: $conf.mqtt_host,
  client_id: $conf.mqtt_client_id
}

if !$conf.mqtt_port.nil?
  $conn_opts["remote_port"] = $conf.mqtt_port
end

if $conf.mqtt_use_auth == true
  $conn_opts["username"] = $conf.mqtt_username
  $conn_opts["password"] = $conf.mqtt_password
end

def main_loop
  $log.info "connecting..."
  MQTT::Client.connect($conn_opts) do |c|
    $log.info "connected!"

    c.subscribe($conf.subscribe_topic)

    c.get do |topic, message|
      #$log.info "recv: topic=#{topic}, message=#{message}"
      json = JSON.parse(message)

      t = Time.iso8601(json["last_update_time"])
      metrics = topic.gsub(/\//, ".")

      s = TCPSocket.open($conf.graphite_host, $conf.graphite_port)

      json.keys.each do |k|
        l = "#{metrics}.#{k} #{json[k]} #{t.to_i}"
        $log.info "write to graphite => #{l}"
        s.puts l
      end

      s.close
    end
  end
end

loop do
  begin
    main_loop
  rescue Exception => e
    $log.debug(e.to_s)
  end

  begin
    Thread.kill($watchdog_thread) if $watchdog_thread.nil? == false
    $watchdog_thread = nil
  rescue
  end

  $log.info "waiting 5 seconds..."
  sleep 5
end
