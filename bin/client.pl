#!/usr/bin/perl

use strict;
use warnings;
use POE qw(Component::Client::AMQP);
use Log::Log4perl;
use Log::Log4perl::Appender;
use Log::Log4perl::Layout;

my $logger;
{
    $logger = Log::Log4perl->get_logger('amqp_client');
    $logger->level($Log::Log4perl::DEBUG);

    my $appender = Log::Log4perl::Appender->new(
        'Log::Log4perl::Appender::Screen',
        stderr => 0,
    );
    $appender->layout(Log::Log4perl::Layout::PatternLayout->new("[\%d] [\%P] \%p: \%m\%n"));
    $logger->add_appender($appender);
}

Net::AMQP::Protocol->load_xml_spec($ARGV[0]);

my $amq = POE::Component::Client::AMQP->create(
    RemoteAddress => '127.0.0.1',
    Logger        => $logger,
);

POE::Session->create(
    inline_states => {
        _start => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];

            $kernel->alias_set('blah');
            $kernel->delay(publish => 1);
        },

        publish => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];
            $amq->channel(1)->queue('one')->publish("Initial test to 'one'");
            $kernel->delay(publish2 => 2);
        },

        publish2 => sub {
            my ($kernel, $heap) = @_[KERNEL, HEAP];
            $amq->channel(1)->queue('two')->publish("Initial test to 'two'");
        },
    },
);

$amq->on_startup(sub {
    my $channel = $amq->channel_create();
    $channel->on_startup(sub {

        $channel->add_periodic_timer(1, sub {
            $logger->info("Sending 'ping' to queue 'one'");
            $channel->queue('one')->publish('ping');
        });

        $channel->queue('one')->subscribe(sub {
            my $msg = shift;
            $logger->info("Queue 'one' received message '$msg'; sending 'pong' to queue 'two'");
            $channel->queue('two')->publish('pong');
        });

        $channel->queue('two')->subscribe(sub {
            my $msg = shift;
            $logger->info("Queue 'two' received message '$msg'");
        });


    });
});


$poe_kernel->run();
