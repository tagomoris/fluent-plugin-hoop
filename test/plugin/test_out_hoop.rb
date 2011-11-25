require 'helper'

class HoopOutputTest < Test::Unit::TestCase
  # setup/teardown and tests of dummy hoop server defined at the end of this class...

  CONFIG = %[
    hoop_server localhost:14000
    path /logs/from/fluentd/foo-%Y%m%d
    username hoopuser
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::TimeSlicedOutputTestDriver.new(Fluent::HoopOutput).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:14000
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:14000
    path /logs/from/fluentd/foo-%Y%m%d%H
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:xxx
    path /logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
    hoop_server hoop.master.local:xxx
    path logs/from/fluentd/foo-%Y%m%d%H
    username hoopuser
      ]
    }

    # config_param :path, :string          # /path/pattern/to/hdfs/file can use %Y %m %d %H %M %S and %T(tag, not-supported-yet)

    d = create_driver(CONFIG)
    
    assert_equal '%Y%m%d', d.instance.time_slice_format

    assert_equal 'localhost:14000', d.instance.hoop_server
    assert_equal '/logs/from/fluentd/foo-%Y%m%d', d.instance.path
    assert_equal 'hoopuser', d.instance.username

    assert_equal true, d.instance.output_time
    assert_equal true, d.instance.output_tag
    assert_equal 'json', d.instance.output_type
    assert_equal true, d.instance.add_newline
    assert_equal 'TAB', d.instance.field_separator
  end

  def test_configure_path_and_slice_format
    d = create_driver(CONFIG)
    assert_equal '%Y%m%d', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%Y%m%d', d.instance.path
    assert_equal '/logs/from/fluentd/foo-20111125', d.instance.path_format('20111125')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/foo-%Y%m
    ]
    assert_equal '%Y%m%d', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%Y%m', d.instance.path
    assert_equal '/logs/from/fluentd/foo-201111', d.instance.path_format('20111125')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/foo-%Y%m%d%H
    ]
    assert_equal '%Y%m%d%H', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%Y%m%d%H', d.instance.path
    assert_equal '/logs/from/fluentd/foo-2011112508', d.instance.path_format('2011112508')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/foo-%Y%m%d%H%M
    ]
    assert_equal '%Y%m%d%H%M', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%Y%m%d%H%M', d.instance.path
    assert_equal '/logs/from/fluentd/foo-201111250811', d.instance.path_format('201111250811')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/foo-%Y%m%d%H%M%S
    ]
    assert_equal '%Y%m%d%H%M%S', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%Y%m%d%H%M%S', d.instance.path
    assert_equal '/logs/from/fluentd/foo-20111125081159', d.instance.path_format('20111125081159')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/foo-%m%d%H
    ]
    assert_equal '%Y%m%d%H', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%m%d%H', d.instance.path
    assert_equal '/logs/from/fluentd/foo-112508', d.instance.path_format('2011112508')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/foo-%M%S.log
    ]
    assert_equal '%Y%m%d%H%M%S', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/foo-%M%S.log', d.instance.path
    assert_equal '/logs/from/fluentd/foo-1159.log', d.instance.path_format('20111125081159')

    d = create_driver CONFIG + %[
path /logs/from/fluentd/%Y%m%d/%H/foo-%M-%S.log
    ]
    assert_equal '%Y%m%d%H%M%S', d.instance.time_slice_format
    assert_equal '/logs/from/fluentd/%Y%m%d/%H/foo-%M-%S.log', d.instance.path
    assert_equal '/logs/from/fluentd/20111125/08/foo-11-59.log', d.instance.path_format('20111125081159')
  end

  def test_format
    d = create_driver
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.expect_format %[2011-11-25T13:14:15Z\ttest\t{"a":1}\n]
    d.expect_format %[2011-11-25T13:14:15Z\ttest\t{"a":2}\n]
    d.run

    d = create_driver CONFIG + %[
output_tag false
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.expect_format %[2011-11-25T13:14:15Z\t{"a":1}\n]
    d.expect_format %[2011-11-25T13:14:15Z\t{"a":2}\n]
    d.run

    d = create_driver CONFIG + %[
output_time false
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.expect_format %[test\t{"a":1}\n]
    d.expect_format %[test\t{"a":2}\n]
    d.run

    d = create_driver CONFIG + %[
output_time false
output_tag false
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.expect_format %[{"a":1}\n]
    d.expect_format %[{"a":2}\n]
    d.run

    d = create_driver CONFIG + %[
output_time false
output_tag false
output_type attr:a
add_newline true # default
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.expect_format %[1\n]
    d.expect_format %[2\n]
    d.run

    d = create_driver CONFIG + %[
output_time false
output_tag false
output_type attr:a
add_newline false
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.expect_format %[1]
    d.expect_format %[2]
    d.run

    d = create_driver CONFIG + %[
output_time false
output_tag false
output_type attr:a,b,c
add_newline true
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2,"c"=>6,"b"=>4}, time)
    d.expect_format %[1\t(NONE)\t(NONE)\n]
    d.expect_format %[2\t4\t6\n]
    d.run

    d = create_driver CONFIG + %[
output_time false
output_tag false
output_type attr:message
add_newline false
    ]
    time = Time.parse("2011-11-25 13:14:15 UTC").to_i
    d.emit({"tag"=>"from.scribe", "message"=>'127.0.0.1 - tagomoris [25/Nov/2011:20:19:04 +0900] "GET http://example.com/api/ HTTP/1.1" 200 39 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.121 Safari/535.2" "-" 71383"' + "\n"}, time)
    d.expect_format '127.0.0.1 - tagomoris [25/Nov/2011:20:19:04 +0900] "GET http://example.com/api/ HTTP/1.1" 200 39 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.121 Safari/535.2" "-" 71383"' + "\n"
    d.run
  end

  def test_write
    d = create_driver

    assert_equal '404', get_code('localhost', 14000, '/logs/from/fluentd/foo-20111124', {'Cookie' => VALID_COOKIE_STRING})

    time = Time.parse("2011-11-24 00:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    paths = d.run
    assert_equal ['/logs/from/fluentd/foo-20111124'], paths
    assert_equal %[2011-11-24T00:14:15Z\ttest\t{"a":1}\n2011-11-24T00:14:15Z\ttest\t{"a":2}\n], get_content('localhost', 14000, paths.first, {'Cookie' => VALID_COOKIE_STRING})
  end

  VALID_COOKIE_STRING = 'alfredo.auth="u=hoopuser&p=hoopuser&t=simple&e=1322203001386&s=SErpv88rOAVEItSOIoCtIV/DSpE="'
  RES_COOKIE_AUTH_FAILURE = WEBrick::Cookie.parse_set_cookie('alfredo.auth=""; Expires=Thu, 01-Jan-1970 00:00:10 GMT; Path=/')
  RES_COOKIE_AUTH_SUCCESS = WEBrick::Cookie.parse_set_cookie(VALID_COOKIE_STRING + '; Version=1; Path=/')
  RES_BODY_STATUS_ROOT = '{"path":"http:\/\/localhost:14000\/","isDir":true,"len":0,"owner":"hoopuser","group":"supergroup","permission":"-rwxr-xr-x","accessTime":0,"modificationTime":1320055230010,"blockSize":0,"replication":0}'
  RES_FORMAT_ALREADY_EXISTS = "{\"statusCode\":500,\"reason\":\"Internal Server Error\",\"message\":\"java.io.IOException: failed to create file %s on client 127.0.0.1 either because the filename is invalid or the file exists\",\"exception\":\"org.apache.hadoop.ipc.RemoteException\"}"
  RES_FORMAT_NOT_FOUND = "{\"statusCode\":404,\"reason\":\"Not Found\",\"message\":\"java.io.FileNotFoundException: failed to append to non-existent file %s on client 127.0.0.1\",\"exception\":\"java.io.FileNotFoundException\"}"
  RES_FORMAT_NOT_FOUND_GET = "{\"statusCode\":404,\"reason\":\"Not Found\",\"message\":\"File does not exist: %s\",\"exception\":\"java.io.FileNotFoundException\"}"

  CONTENT_TYPE_JSON = 'application/json'

  def setup
    Fluent::Test.setup
    @dummy_server_thread = Thread.new do
      srv = if ENV['FLUENT_TEST_DEBUG']
              logger = WEBrick::Log.new('/dev/null', WEBrick::BasicLog::DEBUG)
              WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000, :Logger => logger, :AccessLog => []})
            else
              WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000})
            end
      @fsdata = {}
      begin
        srv.mount_proc('/'){|req,res|
          # status only...
          if req.query['user.name'] or req.cookies.index{|item| item.name == 'alfredo.auth' and item.value}
            res.status = 200
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
            res.body = RES_BODY_STATUS_ROOT
          else
            res.cookies << RES_COOKIE_AUTH_FAILURE
            res.status = 401
          end
        }
        srv.mount_proc('/logs/from/fluentd') {|req, res|
          if req.request_method == 'POST' or req.request_method == 'PUT' or req.request_method == 'DELETE'
            # WEBrick's default handler ignores query parameter of URI without method GET
            req.query.update(Hash[*(req.request_line.split(' ')[1].split('?')[1].split('&').map{|kv|kv.split('=')}.flatten)])
          end
          case
          when (not req.query['user.name'] and req.cookies.index{|item| item.name == 'alfredo.auth' and item.value} < 0)
            res.cookies << RES_COOKIE_AUTH_FAILURE
            res.status = 401
          when (req.query['op'] == 'create' and @fsdata[req.path] and req.query['overwrite'] and req.query['overwrite'] == 'false')
            res.status = 500
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
            res.body = sprintf RES_FORMAT_ALREADY_EXISTS, req.path
          when req.query['op'] == 'create'
            @fsdata[req.path] = req.body
            res.status = 201
            res['Location'] = 'http://localhost:14000' + req.path
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
          when (req.query['op'] == 'append' and @fsdata[req.path])
            @fsdata[req.path] += req.body
            res.status = 200
            res['Location'] = 'http://localhost:14000' + req.path
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
          when req.query['op'] == 'append'
            res.status = 404
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
            res.body = sprintf RES_FORMAT_NOT_FOUND, req.path
          when (req.request_method == 'GET' and @fsdata[req.path]) # maybe GET
            res.status = 200
            res.content_type = 'application/octet-stream'
            res.cookies << RES_COOKIE_AUTH_SUCCESS
            res.body = @fsdata[req.path]
          else
            res.status = 404
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
            res.body = sprintf RES_FORMAT_NOT_FOUND_GET, req.path
          end
        }
        srv.start
      ensure
        srv.shutdown
      end
    end

    # to wait completion of dummy server.start()
    require 'thread'
    cv = ConditionVariable.new
    watcher = Thread.new {
      connected = false
      while not connected
        begin
          get_content('localhost', 14000, '/', {'Cookie' => VALID_COOKIE_STRING})
          connected = true
        rescue Errno::ECONNREFUSED
          sleep 0.1
        rescue StandardError => e
          p e
          sleep 0.1
        end
      end
      cv.signal
    }
    mutex = Mutex.new
    mutex.synchronize {
      cv.wait(mutex)
    }
  end

  def test_dummy_server
    d = create_driver
    authheader = {'Cookie' => VALID_COOKIE_STRING}
    client = Net::HTTP.start(d.instance.hoop_server.split(':')[0], d.instance.hoop_server.split(':')[1])
    assert_equal '401', client.request_get('/').code
    assert_equal '200', client.request_get('/?user.name=hoopuser').code
    assert_equal '200', client.request_get('/', authheader).code

    # /logs/from/fluentd
    path1 = '/logs/from/fluentd/hoge001/moge-access-log'
    path1_line1 = "1111111111111111111111111111111\n"
    path1_line2 = "2222222222222222222222222222222222222222222222222\n"
    assert_equal '404', client.request_put(path1 + '?op=append', path1_line1, authheader).code
    assert_equal '201', client.request_post(path1 + '?op=create&overwrite=false', path1_line1, authheader).code
    assert_equal path1_line1, client.request_get(path1, authheader).body
    assert_equal '200', client.request_put(path1 + '?op=append', path1_line2, authheader).code
    assert_equal path1_line1 + path1_line2, client.request_get(path1, authheader).body

    path2 = '/logs/from/fluentd/hoge002/moge-access-log'
    path2_line1 = "XXXXX___1111111111111111111111111111111\n"
    path2_line2 = "YYYYY___2222222222222222222222222222222222222222222222222\n"
    assert_equal '404', client.request_put(path2 + '?op=append', path2_line1, authheader).code
    assert_equal '201', client.request_post(path2 + '?op=create&overwrite=false', path2_line1, authheader).code
    assert_equal '500', client.request_post(path2 + '?op=create&overwrite=false', path2_line1, authheader).code
    assert_equal path2_line1, client.request_get(path2, authheader).body
    assert_equal '200', client.request_put(path2 + '?op=append', path2_line2, authheader).code
    assert_equal path2_line1 + path2_line2, client.request_get(path2, authheader).body
    assert_equal path2_line1 + path2_line2, get_content('localhost', 14000, path2, authheader)
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end
end
