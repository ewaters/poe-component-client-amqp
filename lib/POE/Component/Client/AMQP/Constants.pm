package POE::Component::Client::AMQP::Constants;
use strict;
use Exporter;
our @ISA = qw(Exporter);

use constant {
    AMQP_ACK    => '__amqp_ack__',
    AMQP_REJECT => '__amqp_reject__',
};

our @EXPORT = qw(AMQP_ACK AMQP_REJECT);

1;
