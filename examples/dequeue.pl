#!/usr/bin/perl

use strict;
use warnings;
use POE;  # for $poe_kernel
use examples;

my $queue_name = 'message_queue';
my $msg_count = $ARGV[0] || 1;

init();

$amq->Logger->info("Dequeueing messages from queue '$queue_name', limit $msg_count (Ctrl-C to stop)");

# Create a queue that will exist after this program ends
my $queue = $channel->queue(
    $queue_name,
    {
        auto_delete => 0, # will remain after all consumers part
        exclusive   => 0, # not limited to just this connection
    },
);

$queue->subscribe(sub {
    my ($message, $meta) = @_;
    $amq->Logger->info("Got '$message'");
    if (--$msg_count == 0) {
        $amq->Logger->info("Stopping as we've reached the limit");
        $amq->stop();
    }
    return 1; # ack it
});

$amq->run();
