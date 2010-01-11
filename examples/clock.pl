#!/usr/bin/perl

use strict;
use warnings;
use examples;
use POE;

init();

$channel
    ->queue('every second')
    ->bind(exchange => 'amq.fanout')
    ->subscribe(sub {
        my $timestamp = shift;
        $amq->Logger->info("every second received " . localtime($timestamp));
    });

$channel
    ->queue('every 5 seconds')
    ->bind(exchange => 'amq.fanout')
    ->subscribe(sub {
        my $timestamp = shift;
        return unless $timestamp % 5 == 0;
        $amq->Logger->info("every 5 seconds received " . localtime($timestamp));
    });

POE::Session->create(
    inline_states => {
        _start => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];

            $kernel->alias_set('blah');
            $kernel->delay(clock => 1);
        },

        clock => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];
            $kernel->delay(clock => 1);

            if (! $amq->is_started) {
                $amq->Logger->error("Server not started; not publishing time to 'amq.fanout'");
                return;
            }

            my $message = time . '';
            $amq->Logger->info("Sending '$message' to exchange 'amq.fanout'");
            
            $poe_kernel->post($channel->{Alias}, server_send =>
                $amq->compose_basic_publish($message,
                    exchange => 'amq.fanout',
                )
            );
        },
    },
);

$amq->run();
