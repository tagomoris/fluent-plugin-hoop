class Fluent::HoopOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('hoop', self)

  config_set_default :buffer_type, 'memory'

  config_param :hoop_server, :string   # host:port
  config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag, not-supported-yet)
  config_param :username, :string      # hoop pseudo username
  
  config_set_default :utc, true
  config_set_default :localtime, false

  config_param :output_time, :bool, :default => true
  config_param :time_format, :string, :default => nil
  config_param :output_tag, :bool, :default => true
  config_param :output_type, :string, :default => 'json' # or 'attr:field' or 'attr:field1,field2,field3(...)'
  config_param :add_newline, :bool,   :default => true
  config_param :field_separator, :string, :default => 'TAB' # or SPACE,COMMA (for output_type=attributes:*)
  # config_param :tag_format, :string, :default => 'all' # or 'last'(last.part.of.tag => tag) or 'none'

  def initialize
    super
    require 'net/http'
  end

  def configure(conf)
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
    # client.request
    $log.info "connected hoop server: #{@host} port #{@port}"
  end

  def shutdown
    super
    @conn.finish
  end

  def record_to_string(record)
    record.to_json
  end

  def format_fullspec(tag, time, record)
    buf = ''
    if @output_time
      buf += @timef.format(time) + @f_separator
    end
    if @output_tag
      buf += tag + @f_separator
    end
    if @output_type == 'json'
      buf += record.json
    else
      buf += @custom_attributes.map{|attr| record[attr]}.join(@f_separator)
    end
    if @add_newline
      buf += "\n"
    end
    buf
  end

  def format(tag, time, record)
    time_str = @timef.format(time)
    time_str + @f_separator + tag + @f_separator + record_to_string(record) + @line_end
  end

  def write(chunk)
    records = []
    chunk.msgpack_each { |record|
      # records << record
    }
    # write records
  end
end
