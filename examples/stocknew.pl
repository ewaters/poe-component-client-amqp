#!perl

use strict;
use warnings;
use examples;
use POE;

$channel->queue('apple stock')->subscribe(
    sub {
	my $price = shift;
        $amq->Logger->info("apple stock: $price");
    }
);

$channel->queue('us stocks')->subscribe(
    sub {
	my ($price, $meta) = @_;
        $amq->Logger->info("us stock: ".$meta->{method_frame}->routing_key." $price");
    }
);

$channel->exchange('stocks', { type => 'topic' } );

$channel->queue('us stocks')->bind('stocks', { routing_key => 'usd.*' });
$channel->queue('apple stock')->bind('stocks', { routing_key => 'usd.appl' });

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

            $kernel->delay(publish_stock_prices => 1);
        },
    },
);

$amq->run;

