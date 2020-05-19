# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import json
import logging
import os
import platform
import subprocess as subp
import sys
from time import sleep

from PyInquirer import Validator, ValidationError
from PyInquirer import style_from_dict, Token, prompt
from pyfiglet import Figlet

try:
    from sampleConfig import readme, myCredentials
except ImportError:
    from .sampleConfig import readme, myCredentials


def init():
    print()
    f = Figlet(font='slant')
    print(f.renderText('GDA Score'))

    print(
        'Welcome to GDA Score project. After you provided a path for local configuration folder through this ' +
        'process, You can modify its default configuration.')
    print()

    style = style_from_dict({
        Token.QuestionMark: '#E91E63 bold',
        Token.Selected: '#673AB7 bold',
        Token.Instruction: '#FF0000',  # default
        Token.Answer: '#2196f3 bold',
        Token.Question: '#fff000',
    })

    class PathValidator(Validator):
        def validate(self, document):
            ok = os.path.exists(document.text)
            if not ok:
                raise ValidationError(
                    message='path does not exist. please enter a valid path.',
                    cursor_position=len(document.text))  # Move cursor to end

    questions = [
        {
            'type': 'input',
            'name': 'cnfPath',
            'message': 'where should "config" folder be placed? please provide absolute path.',
            'validate': PathValidator,
            'default': os.path.abspath(os.getcwd())
        },
        {
            'type': 'list',
            'name': 'boolCrtCnfFld',
            'message': 'there is no folder named "config" in the path specified, it will be created. ' +
                       'hit enter to continue',
            'choices': ['ok'],
            'when': lambda ans: not os.path.exists(os.path.join(ans['cnfPath'], 'config'))
        },
        {
            'type': 'list',
            'name': 'crtEnvVars',
            'message': 'Set required environment variables for working with rawDB and Aircloak?' +
                       ' (see README.md for more explanation.)',
            'choices': ['yes', 'no'],
        }
    ]

    answers = prompt(questions, style=style)
    print()
    main(**answers)
    print('\ninitiation phase finished.\n')


def main(cnfPath, crtEnvVars, boolCrtCnfFld=None):
    if boolCrtCnfFld:
        createConfigFolder(cnfPath)
    if crtEnvVars == 'yes':
        setEnvVars()
    # NOTE: after env vars no need to have myCredentials.json so line below is commented out
    # propagateSampleConfig(cnfPath)
    setGlobalVariables(cnfPath)


def createConfigFolder(cnfPath):
    try:
        os.mkdir(os.path.join(cnfPath, 'config'))
    except IOError as ex:
        print(f'result folder could not be created due to an error: {ex}')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)
    else:
        print(' > config folder created successfully. ')
        sleep(1)

def setEnvVars():
    if platform.system() == "Windows":
        r0 = subp.call(['setx', 'GDA_SCORE_DIFFIX_USER', 'gda-score_ro_user'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        r1 = subp.call(['setx', 'GDA_SCORE_DIFFIX_PASS', 'moquaiR7'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        r2 = subp.call(['setx', 'GDA_SCORE_RAW_USER', 'gda-score_ro_user'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        r3 = subp.call(['setx', 'GDA_SCORE_RAW_PASS', 'moquaiR7'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        if not (r0 and r1 and r2 and r3):
            print(f" > windows environment variables has been set successfully. before using gdascore, "
                  f"you may need to open another command prompt for changes to be effective.")
        else:
            logging.error(" setting environment variables failed. please set them manually.")

    elif platform.system() == "Linux":
        r0 = subp.call(['export', 'GDA_SCORE_DIFFIX_USER=gda-score_ro_user'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        r1 = subp.call(['export', 'GDA_SCORE_DIFFIX_PASS=moquaiR7'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        r2 = subp.call(['export', 'GDA_SCORE_RAW_USER=gda-score_ro_user'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        r3 = subp.call(['export', 'GDA_SCORE_RAW_PASS=moquaiR7'], stdout=subp.DEVNULL,
                       stderr=subp.STDOUT)
        if not (r0 and r1 and r2 and r3):
            print(f" > linux environment variables has been set successfully. you may need to open another"
                  f"terminal for changes to be effective.")
        else:
            logging.error("setting environment variables failed. please set them manually.")
    else:
        logging.error(" setting environment variables failed because platform in not recognized"
                      " as windows or linux. do set variables manually.")

# py myCredentials.json in config folder
def propagateSampleConfig(cnfPath):
    try:
        with open(os.path.join(cnfPath, 'config', 'myCredentials.json'), 'w') as f:
            f.write(myCredentials)
    except IOError as e:
        # print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print(f'propagating default config files failed due to an error: {e} \n')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)

    except:  # handle other exceptions such as attribute errors
        print("Unexpected error:", sys.exc_info()[0])
        exit(-1)
    else:
        print(' > default config files propagated successfully.')
        sleep(1)

def setGlobalVariables(cnfPath):
    to_write_cnfPath = json.dumps(os.path.abspath(cnfPath))
    res = f'''{{
            "config_path": {to_write_cnfPath}
    }}'''
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'global_config', 'config_var.json'),
                  'w') as f:
            f.write(res)
    except IOError as e:
        # print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print(f'setting global configuration variables failed due to an error: {e} \n')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)
    else:
        print(' > global configuration variables set successfully.')
        sleep(1)


if __name__ == "__main__":
    init()
