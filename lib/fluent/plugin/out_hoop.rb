module PlainTextFormatterMixin
  # config_param :output_data_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'

  attr_accessor :output_include_time, :output_include_tag, :output_data_type
  attr_accessor :add_newline, :field_separator
  
  def configure(conf)
    super

    @output_include_time ||= true
    @output_include_tag ||= true
    @output_data_type ||= 'json'

    @field_separator = case @field_separator
                       when 'SPACE' then ' '
                       when 'COMMA' then ','
                       else "\t"
                       end
    @add_newline = Config.bool_value(@add_newline)

    if (not @localtime) and @utc
      @localtime = false
    end
    # mix-in default time formatter (or you can overwrite @timef on your own configure)
    @timef = @output_include_time ? Fluent::TimeFormatter.new(@time_format, @localtime) : nil

    @custom_attributes = []
    if @output_data_type == 'json'
      self.instance_eval {
        def stringify_record(record)
          record.to_json
        end
      }
    elsif @output_data_type =~ /^attr:(.*)$/
      @custom_attributes = $1.split(',')
      if @custom_attributes.size > 1
        self.instance_eval {
          def stringify_record(record)
            @custom_attributes.map{|attr| (record[attr] || 'NULL').to_s}.join(@field_separator)
          end
        }
      elsif @custom_attributes.size == 1
        self.instance_eval {
          def stringify_record(record)
            (record[@custom_attributes[0]] || 'NULL').to_s
          end
        }
      else
        raise Fluent::ConfigError, "Invalid attributes specification: '#{@output_data_type}', needs one or more attributes."
      end
    else
      raise Fluent::ConfigError, "Invalid output_data_type: '#{@output_data_type}'. specify 'json' or 'attr:ATTRIBUTE_NAME' or 'attr:ATTR1,ATTR2,...'"
    end

    if @output_include_time and @output_include_tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record)
            @timef.format(time) + @field_separator + tag + @field_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record)
            @timef.format(time) + @field_separator + tag + @field_separator + stringify_record(record)
          end
        }
      end
    elsif @output_include_time
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            @timef.format(time) + @field_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            @timef.format(time) + @field_separator + stringify_record(record)
          end
        }
      end
    elsif @output_include_tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            tag + @field_separator + stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            tag + @field_separator + stringify_record(record)
          end
        }
      end
    else # without time, tag
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            stringify_record(record) + "\n"
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            stringify_record(record)
          end
        }
      end
    end
  end

  def stringify_record(record)
    record.to_json
  end

  def format(tag, time, record)
    time_str = @timef.format(time)
    time_str + @field_separator + tag + @field_separator + stringify_record(record) + "\n"
  end

end

class Fluent::HoopOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('hoop', self)

  config_set_default :buffer_type, 'memory'
  config_set_default :time_slice_format, '%Y%m%d' # %Y%m%d%H
  # config_param :tag_format, :string, :default => 'all' # or 'last'(last.part.of.tag => tag) or 'none'

  config_param :hoop_server, :string   # host:port
  config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag, not-supported-yet)
  config_param :username, :string      # hoop pseudo username
  
  config_set_default :utc, true
  config_set_default :localtime, false

  # config_param :output_time, :bool, :default => true
  # config_param :output_tag, :bool, :default => true
  # config_param :output_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'
  # config_param :add_newline, :bool,   :default => true
  # config_param :field_separator, :string, :default => 'TAB' # or SPACE,COMMA (for output_type=attributes:*)
  include PlainTextFormatterMixin

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

    @f_separator = case @field_separator
                   when 'SPACE' then ' '
                   when 'COMMA' then ','
                   else "\t"
                   end

    if @utc
      @localtime = false
    end
    @timef = @output_time ? Fluent::TimeFormatter.new(@time_format, @localtime) : nil

    @line_end = @add_newline ? "\n" : ""

    # config_param :output_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'
    @custom_attributes = []
    if @output_type == 'json'
      # default record_to_string
    elsif @output_type =~ /^attr:(.*)$/
      @custom_attributes = $1.split(',')
      if @custom_attributes.size > 1
        self.instance_eval {
          def record_to_string(record); @custom_attributes.map{|attr| (record[attr] || '(NONE)').to_s}.join(@f_separator); end
        }
      elsif @custom_attributes.size == 1
        self.instance_eval { def record_to_string(record); (record[@custom_attributes[0]] || '(NONE)').to_s; end }
      else
        raise Fluent::ConfigError, "Invalid attributes specification: '#{@output_type}', needs one or more attributes."
      end
    else
      raise Fluent::ConfigError, "Invalid output_type: '#{@output_type}'. specify 'json' or 'attr:ATTRIBUTE_NAME' or 'attr:ATTR1,ATTR2,...'"
    end

    if @output_time and @output_tag
      # default format method
    elsif @output_time
      self.instance_eval {
        def format(tag,time,record);
          time_str = @timef.format(time) ; time_str + @f_separator + record_to_string(record) + @line_end
        end
      }
    elsif @output_tag
      self.instance_eval {
        def format(tag,time,record);
          tag + @f_separator + record_to_string(record) + @line_end
        end
      }
    else
      if @add_newline
        self.instance_eval {
          def format(tag,time,record);
            record_to_string(record) + @line_end
          end
        }
      else
        self.instance_eval {
          def format(tag,time,record);
            record_to_string(record)
          end
        }
      end
    end
  end

  def start
    super
    @conn = Net::HTTP.start(@host, @port)
    begin
      res = @conn.request_get("/?op=status&user.name=#{@username}")
      @authorized_header = {'Cookie' => res['Set-Cookie'].split(';')[0], 'Content-Type' => 'application/octet-stream'}
    rescue
      $log.error "failed to connect hoop server: #{@host} port #{@port}"
      raise
    end
    $log.info "connected hoop server: #{@host} port #{@port}"
  end

  def shutdown
    super
    @conn.finish
  end

  def record_to_string(record)
    record.to_json
  end

  # def format_fullspec(tag, time, record)
  #   buf = ''
  #   if @output_time
  #     buf += @timef.format(time) + @f_separator
  #   end
  #   if @output_tag
  #     buf += tag + @f_separator
  #   end
  #   if @output_type == 'json'
  #     buf += record.json
  #   else
  #     buf += @custom_attributes.map{|attr| record[attr]}.join(@f_separator)
  #   end
  #   if @add_newline
  #     buf += "\n"
  #   end
  #   buf
  # end

  def format(tag, time, record)
    time_str = @timef.format(time)
    time_str + @f_separator + tag + @f_separator + record_to_string(record) + @line_end
  end

  def path_format(chunk_key)
    # p({:time_slice_format => @time_slice_format, :path => @path, :chunk_key => chunk_key})
    if chunk_key.length < 1
      raise RuntimeError
    end
    Time.strptime(chunk_key, @time_slice_format).strftime(@path)
  end

  def write(chunk)
    hdfs_path = path_format(chunk.key)
    begin
      res = @conn.request_put(hdfs_path + "?op=append", chunk.read, @authorized_header)
      if res.code == '404'
        res = @conn.request_post(hdfs_path + "?op=create&overwrite=false", chunk.read, @authorized_header)
        if res.code == '500'
          res = @conn.request_put(hdfs_path + "?op=append", chunk.read, @authorized_header)
        end
      end
      if res.code != '200' and res.code != '201'
        $log.warn "failed to write data to path: #{hdfs_path}, code: #{res.code} #{res.message}"
      else
        @authorized_header['Cookie'] = res['Set-Cookie'].split(';')[0]
      end
    rescue
      $log.error "failed to communicate server, #{@host} port #{@port}, path: #{hdfs_path}"
      raise
    end
    hdfs_path
  end
end
