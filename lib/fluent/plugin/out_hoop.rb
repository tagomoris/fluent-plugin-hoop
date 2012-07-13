require 'fluent/mixin/plaintextformatter'

class Fluent::HoopOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('hoop', self)

  config_set_default :buffer_type, 'memory'
  config_set_default :time_slice_format, '%Y%m%d' # %Y%m%d%H

  config_param :hoop_server, :string   # host:port
  config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag, not-supported-yet)
  config_param :username, :string      # hoop pseudo username
  
  include Fluent::Mixin::PlainTextFormatter

  def initialize
    super
    require 'net/http'
    require 'time'
  end

  def configure(conf)
    if conf['path']
      if conf['path'].index('%S')
        conf['time_slice_format'] = '%Y%m%d%H%M%S'
      elsif conf['path'].index('%M')
        conf['time_slice_format'] = '%Y%m%d%H%M'
      elsif conf['path'].index('%H')
        conf['time_slice_format'] = '%Y%m%d%H'
      end
    end

    super

    unless /\A([a-zA-Z0-9][-a-zA-Z0-9.]*):(\d+)\Z/ =~ @hoop_server
      raise Fluent::ConfigError, "Invalid config value on hoop_server: '#{@hoop_server}', needs SERVER_NAME:PORT"
    end
    @host = $1
    @port = $2.to_i
    unless @path.index('/') == 0
      raise Fluent::ConfigError, "Path on hdfs MUST starts with '/', but '#{@path}'"
    end
    @conn = nil
    @header = {'Content-Type' => 'application/octet-stream'}
  end

  def start
    super

    # okey, net/http has reconnect feature. see test_out_hoop_reconnect.rb
    conn = Net::HTTP.start(@host, @port)
    begin
      res = conn.request_get("/?op=status&user.name=#{@username}")
      if res.code.to_i < 300 and res['Set-Cookie']
        @authorized_header = {'Cookie' => res['Set-Cookie'].split(';')[0], 'Content-Type' => 'application/octet-stream'}
      else
        $log.error "initalize request failed, code: #{res.code}, message: #{res.body}"
        raise Fluent::ConfigError, "initalize request failed, code: #{res.code}, message: #{res.body}"
      end
    rescue
      $log.error "failed to connect hoop server: #{@host} port #{@port}"
      raise
    end
    conn.finish
    $log.info "connected hoop server: #{@host} port #{@port}"
  end

  def shutdown
    super
  end

  def record_to_string(record)
    record.to_json
  end

  # def format(tag, time, record)
  # end

  def path_format(chunk_key)
    Time.strptime(chunk_key, @time_slice_format).strftime(@path)
  end

  def send_data(path, data, retries=0)
    conn = Net::HTTP.start(@host, @port)
    conn.read_timeout = 5
    res = conn.request_put(path + "?op=append", data, @authorized_header)
    if res.code == '401'
      res = conn.request_get("/?op=status&user.name=#{@username}")
      if res.code.to_i < 300 and res['Set-Cookie']
        @authorized_header = {'Cookie' => res['Set-Cookie'].split(';')[0], 'Content-Type' => 'application/octet-stream'}
      else
        $log.error "Failed to update authorized cookie, code: #{res.code}, message: #{res.body}"
        raise Fluent::ConfigError, "Failed to update authorized cookie, code: #{res.code}, message: #{res.body}"
      end
      res = conn.request_put(path + "?op=append", data, @authorized_header)
    end
    if res.code == '404'
      res = conn.request_post(path + "?op=create&overwrite=false", data, @authorized_header)
    end
    if res.code == '500'
      if retries >= 3
        raise StandardError, "failed to send_data with retry 3 times InternalServerError"
      end
      sleep 0.3 # yes, this is a magic number
      res = send_data(path, data, retries + 1)
    end
    conn.finish
    if res.code != '200' and res.code != '201'
      $log.warn "failed to write data to path: #{path}, code: #{res.code} #{res.message}"
    end
    res
  end

  def write(chunk)
    hdfs_path = path_format(chunk.key)
    begin
      send_data(hdfs_path, chunk.read)
    rescue
      $log.error "failed to communicate server, #{@host} port #{@port}, path: #{hdfs_path}"
      raise
    end
    hdfs_path
  end
end
