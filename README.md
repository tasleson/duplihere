# duplihere
Copy & Paste finder for source files or any utf-8 text files.


### Why

A number of different copy and paste detectors exist.  Some examples include:

* [PMD](https://pmd.github.io/)
* [Simian](http://www.harukizaemon.com/simian/)
* [tctoolkit](https://bitbucket.org/nitinbhide/tctoolkit/src/default/)

So why write another?  I've wanted a simple tool, one that works like simian,
but is open source and free for anyone. Thus this project was born.  In
general I think writing a lexer and tokenizing the source isn't needed.
There is a ton of code that is very much copy and pasted verbatim.
Developers are lazy, they don't change things :-)

### How to use

```bash
$ duplihere --help
duplihere - 0.2.0 - find duplicate text

usage: duplihere [-p -l <number>] -f <pattern 1> <pattern n> 

Find duplicate lines of text in one or more text files.

The duplicated text can be at different levels of indention,
but otherwise needs to be identical.

More information: https://github.com/tasleson/duplihere

argument:                                      description
    -p, --print                                print duplicate text [default: false]
    -l, --lines <number>                       minimum number of duplicate lines [default: 6]
    -f, --files <pattern 1> <pattern n>        1 or more file pattern(s), eg. "**/*.[h|c]" "*.py" [required]

```

An example ...
```bash
$ duplihere -l 10 -p -f '/home/user/somewhere/**/*.py' '/tmp/*.py'
```


### Status

Very early development with a questionable algorithm and implementation,
but it does appear to provide some useful results.  Graph of memory and CPU
consumption while looking through linux kernel source tree.

![duplihere](https://user-images.githubusercontent.com/2520480/70284095-7c805e00-1788-11ea-9554-9060ad5e1ae1.png)
