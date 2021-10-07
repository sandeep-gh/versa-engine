
import sys
from scripts.pagefromfile import PageFromFileReader, PageFromFileRobot
# https://stackoverflow.com/questions/8315389/how-do-i-print-functions-as-they-are-called

#import os


# def tracefunc(frame, event, arg, indent=[0]):
#     if event == "call":
#         indent[0] += 2
#         if "pywikibot" in frame.f_code.co_filename:
#             print("-" * indent[0] + "> call function",
#                   frame.f_code.co_filename, frame.f_code.co_name)
#     elif event == "return":
#         if "pywikibot" in frame.f_code.co_filename:
#             print("<" + "-" * indent[0], "exit function",
#                   frame.f_code.co_filename,  frame.f_code.co_name)
#         indent[0] -= 2
#     return tracefunc


# sys.setprofile(tracefunc)


filename = "testtext.txt"
r_options = {}
r_options['title'] = "botwashere29"
options = {}
options['summary'] = "bot from versa"
options['always'] = True
reader = PageFromFileReader(filename, **r_options)
# for res in reader:
#    print(res)

#reader = PageFromFileReader(filename, **r_options)
bot = PageFromFileRobot(generator=reader, **options)
bot.run()
