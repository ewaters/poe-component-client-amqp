use strict;
use warnings;
use Test::More tests => 2;

BEGIN {
    use_ok('POE::Component::Client::AMQP');
}

my $client = POE::Component::Client::AMQP->create(
    RemoteAddress => '127.0.0.1',
    is_testing => 1, # don't create POE sessions
);

isa_ok($client, 'POE::Component::Client::AMQP');
