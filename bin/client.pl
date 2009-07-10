#!/usr/bin/perl

use strict;
use warnings;
use POE qw(Component::Client::AMQP);

# Libraries for the dumper() calls
use YAML::XS;
use Net::AMQP::Common qw(show_ascii);
use Term::ANSIColor qw(:constants);

my $debug = $ENV{DEBUG};

Net::AMQP::Protocol->load_xml_spec($ARGV[0]);

my $amq = POE::Component::Client::AMQP->create(
    RemoteAddress => '127.0.0.1',

    ($debug ? (
    Debug         => {
        logic        => 1,

        frame_input  => 1,
        frame_output => 1,
        frame_dumper => sub {
            my $output = YAML::XS::Dump(shift);
            chomp $output;
            return "\n" . BLUE . $output . RESET;
        },

        raw_input    => 1,
        raw_output   => 1,
        raw_dumper => sub {
            my $raw = shift;
            my $output = "raw [".length($raw)."]: ".show_ascii($raw);
            return "\n" . YELLOW . $output . RESET;
        },
    },
    ) : ()),
);

my $channel = $amq->channel();

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
            $channel->queue('one')->publish('ping');
            $amq->Logger->info("Sending 'ping' to queue 'one'");
            $kernel->delay(ping => 1);
        },
    },
);

$amq->run();
