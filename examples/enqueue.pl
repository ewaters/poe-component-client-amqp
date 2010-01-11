#!/usr/bin/perl

use strict;
use warnings;
use POE;  # for $poe_kernel
use examples;

my $queue_name = 'message_queue';
my @msg = @ARGV;
my $msg_count = 0;

if (int @msg == 0) {
    die "Call with a list of messages to enqueue";
}

init(
    Callbacks => {
        FrameSent => [ sub {
            my ($amq, $frame) = @_;
            return unless ref $frame eq 'Net::AMQP::Frame::Body';
            $amq->Logger->info("Detected a Body frame being sent; current count: " . ($msg_count + 1));
            if (++$msg_count == int @msg) {
                $amq->Logger->info("Disconnecting, as we've sent all the messages");
                $poe_kernel->post($amq->{Alias}, 'server_disconnect');
            }
        } ],
    },
);

# Create a queue that will exist after this program ends
my $queue = $channel->queue(
    $queue_name,
    {
        auto_delete => 0, # will remain after all consumers part
        exclusive   => 0, # not limited to just this connection
    },
);

# Publish a few things
for (my $i = 0; $i <= $#msg; $i++) {
    my $msg = $msg[$i];
    $queue->publish($msg);
    $amq->Logger->info("Deferred publish '$msg' to queue '$queue_name'");
}

$amq->run();
