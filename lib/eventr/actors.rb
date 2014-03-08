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

require 'singleton'
require 'monitor'

module Eventr
  class SupervisedObject
    attr_reader :on_exception

    def stop
      threads.values.each { |t| t.send :kill }
    end

    def start
      start_application_thread
      start_supervisor_thread
    end

    def threads
      @threads ||= {}
    end

    def application
      threads[:application]
    end

    def supervisor
      threads[:supervisor]
    end

    def on_exception=(&block) # rubocop:disable TrivialAccessors
      @on_exception = block
    end

    def sleep_time_from_backoff
      backoff = Thread.current[:backoff] || 0
      (0..backoff).inject([1, 0]) { |(a, b), _| [b, a + b] }[0]
    end

    def start_application_thread
      threads[:application] ||= Thread.new do
        begin
          main
        rescue StandardError => e
          on_exception.call(e) if on_exception.respond_to? :call
          warn "#{e.class.name}: #{e.message}\n\t#{e.backtrace.join("\n\t")}"
          raise e
        ensure
          threads[:supervisor].wakeup # wakeup the supervisor to help us recover
        end
      end
    end

    def start_supervisor_thread # rubocop:disable MethodLength
      threads[:supervisor] ||= Thread.new do
        Thread.current[:backoff] = 1

        begin
          runs = 5
          loop do
            unless application && application.alive?
              puts "#{self.class.name}::Supervisor: cleaning up app thread and restarting it."
              threads[:application] = nil
              start_application_thread

              # stop when we've successfully cleaned something up
              runs = 0

              # and make sure to reset backoff
              Thread.current[:backoff] = 1
            end

            # check for required cleanup 5 times over as many seconds
            if (runs -= 1) <= 0
              Thread.stop
              runs = 5
            end

            sleep 1
          end

        rescue StandardError => e
          warn "#{e.class.name}: #{e.message}\n\t#{e.backtrace.join("\n\t")}"

          if Thread.current[:backoff] <= 15
            Thread.current[:backoff] += 1
            sleep_time = sleep_time_from_backoff
            warn "sleeping for #{sleep_time} before restarting supervisor"
            sleep sleep_Time
            retry
          end

          # if the supervisor goes away, take the whole thing down.
          error_msg = "supervisor went away due to: #{e.class.name}: #{e.message} -> #{e.backtrace.first}"
          threads[:application].raise Error::SupervisorDown, error_msg

          raise e
        end
      end
    end
  end

  class Publisher < SupervisedObject
    attr_reader :block, :events
    private :block, :events

    def initialize(&block)
      @block   = block || method(:default_loop)
      @events  = Queue.new
    end

    def pop(non_block = false)
      events.pop(non_block)
    end
    alias_method :shift, :pop

    def main
      block.call(events)
    end

    def push(event)
      @events << event
    end
    alias_method :publish, :push

    def default_loop(events)
      loop { Thread.stop }
    end
    private :default_loop
  end

  class Consumer < SupervisedObject
    attr_reader :block, :publisher
    private :block, :publisher

    def initialize(publisher, &block)
      @publisher = publisher
      @block     = block
    end

    def main
      loop { block.call(publisher.pop) }
    end
  end
end
