

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from mrjob.examples.mr_word_freq_count import MRWordFreqCount
import sys
from operator import itemgetter, attrgetter, methodcaller
WORD_RE = re.compile(r"[\w']+")

list = []
if __name__ == '__main__':
    job = MRWordFreqCount(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = job.parse_output_line(line)
            object = (key, value)
            list.append(object)

    sorted_list = sorted(list, key=itemgetter(1), reverse=True)
    print sorted_list[0:10]
