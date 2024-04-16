sub MAIN(Str :$host = '0.0.0.0', Int :$port = 3333) {

    my $id = 1;
    my $is-last = "false";
    my $sum = 0;

    react {
        whenever IO::Socket::Async.listen($host, $port) -> $conn {
            react {
                my Bool:D $ignore = True;

                whenever Supply.interval(60).rotor(1, 1 => 1) {
                    $ignore = !$ignore;
                }

                whenever Supply.interval(1) {
                    next if $ignore;

                    my $greet = ('a' .. 'z').pick(1).join;
                    $sum += 1;
                    if $sum % 30 == 0 {
                       $is-last = "true";
                    } else {
                       $is-last = "false";
                    }

                    print sprintf("\{'id':%d,'greet':'%s','isLast':%s}\n", $id, $greet, $is-last);
                    $conn.print: sprintf("\{'id':%d,'greet':'%s','isLast':%s}\n", $id, $greet, $is-last);
                }

                whenever signal(SIGINT) {
                    say "Done.";
                    done;
                }
            }
        }
        CATCH {
            default {
                say .^name, ': ', .Str;
                say "handled in $?LINE";
            }
        }
    }
}
