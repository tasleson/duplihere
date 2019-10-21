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
Usage:	duplihere [--help ] [--lines LINES] [--files FILES...] [--print ] 
Options:

--help (-h)	Required: false	Type: Flag
	Show this help message

--lines (-l)	Required: false	Type: Option
	Minimum number of duplicate lines, default 6

--files (-f)	Required: true	Type: List
	File pattern(s)

--print (-p)	Required: false	Type: Flag
	Print duplicate text

```

An example ...
```bash
$ duplihere -l 10 -p -f '/home/user/somewhere/**/*.py' '/tmp/*.py'
```


### Status

Very early development with a questionable algorithm and implementation,
but it does appear to provide some useful results.  It gobbles up lots of
memory and performance suffers if the code/text has lots duplicated.
