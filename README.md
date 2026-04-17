This project will analyze and visualize the change of University of Toronto
Department of Computer Science's faculty over the years.

The cs.toronto.edu website has changed over the years. Using waybackmachine we
can scrape old listings of the faculty back to at least 2007. The particular
subpage URL where faculty are listed has changed. Here're a few samples:

Nov 2007 https://web.archive.org/web/20071130022225/http://web.cs.toronto.edu/dcs/index.php?section=95
Jan 2009 https://web.archive.org/web/20090115162822/http://web.cs.toronto.edu/dcs/index.php?section=95
Sep 2009 https://web.archive.org/web/20090901115327/http://web.cs.toronto.edu/people/faculty.htm
Dec 2017 https://web.archive.org/web/20171206022938/http://web.cs.toronto.edu/people/faculty.htm
Feb 2018 https://web.archive.org/web/20190225060452/http://web.cs.toronto.edu/people/faculty.htm
Jul 2019 https://web.archive.org/web/20190723161443/http://web.cs.toronto.edu/people/faculty.htm
Feb 2020 https://web.archive.org/web/20190723161443/http://web.cs.toronto.edu/people/faculty.htm
Nov 2020 https://web.archive.org/web/20201127151540/https://web.cs.toronto.edu/contact-us/faculty-directory
Oct 2020 https://web.archive.org/web/20211028164500/https://web.cs.toronto.edu/contact-us/faculty-directory
Nov 2021 https://web.archive.org/web/20211123015849/https://web.cs.toronto.edu/people/faculty-directory
Apr 2026 https://web.cs.toronto.edu/people/faculty-directory

The first step is to write a python program to attempt to scrape and collect faculty directory pages every month
starting with Nov 2007 using the waybackmachine.

The next step is to process this scraped data use fuzzy matching to track
individual faculty across these pages. We need to track the first date that they
appear on the list and whether they appear as tenure (research) track or
teaching stream. CLTA, Adjucts, and Emeritus should be skipped.

Finally output some visualizations and summaries of the data found so far:
growth of each stream over time. +/- plots for each year, etc.
