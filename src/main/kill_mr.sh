#!/usr/bin/env bash

pgrep -f "mrworker|mrcoordinator|test-mr" | xargs kill

