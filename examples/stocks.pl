#!/usr/bin/perl

use strict;
use warnings;
use examples;
use POE;

init();

# Enqueue the Queue.Declare calls first

$channel
    ->queue('apple stock')
    ->subscribe(sub {
        my $price = shift;
        $amq->Logger->info("apple stock: $price");
    });

$channel
    ->queue('us stocks')
    ->subscribe(sub {
        my ($price, $meta) = @_;
        $amq->Logger->info("us stock: ".$meta->{method_frame}->routing_key." $price");
    });

# Next, enqueue the Exchange.Declare and Queue.Bind calls (the Queue.Declare calls,
# being asynchronous, will have been defined by this point)

$channel->send_frames(
    Net::AMQP::Protocol::Exchange::Declare->new(
        exchange => 'stocks',
        type => 'topic',
    ),
    Net::AMQP::Protocol::Queue::Bind->new(
        queue => 'apple stock',
        exchange => 'stocks',
        routing_key => 'usd.appl',
    ),
    Net::AMQP::Protocol::Queue::Bind->new(
        queue => 'us stocks',
        exchange => 'stocks',
        routing_key => 'usd.*',
    ),
);

# Setup an interval for publishing stock prices

POE::Session->create(
    inline_states => {
        _start => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];

            $kernel->alias_set('stocks.pl');

            # Delay before starting the publish, as AMQP needs time to start up
            $kernel->delay(publish_stock_prices => 1);
        },

        publish_stock_prices => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];
            $kernel->delay(publish_stock_prices => 1);

            if (! $amq->is_started) {
                $amq->Logger->error("Server not started; not publishing stock prices");
                return;
            }

            my %stocks = (
                appl => 170 + rand(1000) / 100.0,
                msft => 22 + rand(500) / 100.0,
            );
            while (my ($stock, $price) = each %stocks) {
                $price = sprintf '%.2f', $price;

                $amq->Logger->info("publishing $stock $price");

                $channel->send_frames(
                    $amq->compose_basic_publish(
                        $price,
                        exchange => 'stocks',
                        routing_key => 'usd.' . $stock,
                    )
                );
            }
        },
    },
);

$amq->run();

__END__
[Wed Aug 26 07:28:01 2009] INFO: Connected to the AMQP server and ready to act
[Wed Aug 26 07:28:02 2009] INFO: publishing appl 171.62
[Wed Aug 26 07:28:02 2009] INFO: publishing msft 22.08
[Wed Aug 26 07:28:02 2009] INFO: us stock: usd.appl 171.62
[Wed Aug 26 07:28:02 2009] INFO: apple stock: 171.62
[Wed Aug 26 07:28:02 2009] INFO: us stock: usd.msft 22.08

[Wed Aug 26 07:28:03 2009] INFO: publishing appl 176.38
[Wed Aug 26 07:28:03 2009] INFO: publishing msft 26.41
[Wed Aug 26 07:28:03 2009] INFO: us stock: usd.appl 176.38
[Wed Aug 26 07:28:03 2009] INFO: apple stock: 176.38
[Wed Aug 26 07:28:03 2009] INFO: us stock: usd.msft 26.41

