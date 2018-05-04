## TODO: remove this patch when datastax/ruby-driver release 3.2.3 is out
require 'ione'
require 'cassandra'

class Cassandra::Protocol::CqlProtocolHandler
  class RequestPromise < Ione::Promise
    old_constructor = instance_method(:initialize)
    define_method(:initialize) do |request, timeout, scheduler|
      @timer = nil
      old_constructor.bind(self).(request, timeout, scheduler)
    end
  end
end
