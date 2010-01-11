#!/usr/bin/perl

use strict;
use warnings;
use examples;
use POE;

init();

$channel->queue('one')->subscribe(sub {
    my $msg = shift;
    $amq->Logger->info("Queue 'one' received message '$msg'; sending 'pong' to queue 'two'");
    $channel->queue('two')->publish('pong');
});

$channel->queue('two')->subscribe(sub {
    my $msg = shift;
    $amq->Logger->info("Queue 'two' received message '$msg'\n ");
});

POE::Session->create(
    inline_states => {
        _start => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];

            $kernel->alias_set('blah');
            $kernel->delay(ping => 1);
        },

        ping => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];
            $kernel->delay(ping => 1);

            if (! $amq->is_started) {
                $amq->Logger->error("Server not started; not sending 'ping'");
                return;
            }

            $channel->queue('one')->publish('ping');
            $amq->Logger->info("Sending 'ping' to queue 'one'");
        },
    },
);

$amq->run();
