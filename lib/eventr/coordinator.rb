=begin
Copyright (C) 2013 Carl P. Corliss

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
=end

require 'eventr/errors'
require 'eventr/actors'

module Eventr
  class Coordinator
    include Singleton

    attr_reader :publishers, :consumers

    def initialize
      @publishers = {}
      @consumers  = {}
    end

    def self.method_missing(method, *args, &block)
      return super unless self.instance.respond_to?(method)
      instance.public_send(method, *args, &block)
    end

    def publisher(queue_name, &block)
      raise InvalidQueue, "publisher already defined for queue '#{queue_name}'" if @publishers.include? queue_name
      @publishers[queue_name] = Publisher.new(&block)
    end

    def publish(queue_name, event)
      raise InvalidQueue, "Publisher #{queue_name.inspect} doesn't exist" unless @publishers.include? queue_name
      @publishers[queue_name].push(event)
    end

    def consumer(queue_name, &block)
      raise InvalidQueue, "#{queue_name} queue does not exist. Define a publisher for the queue first." unless @publishers.include? queue_name
      @consumers[queue_name] ||= []
      @consumers[queue_name] << Consumer.new(@publishers[queue_name], &block)
    end

    def start
      @publishers.each do |queue_name, _pubisher|
        _pubisher.start
        @consumers[queue_name].each { |c| c.start }
      end
    end

    def stop
      @publishers.each { |q, p| p.stop; @consumers[q].each { |c| c.stop } }
    end
  end
end
