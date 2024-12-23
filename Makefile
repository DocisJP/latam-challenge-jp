.PHONY: all help q1-time q1-memory q2-time q2-memory q3-time q3-memory q1-time-detailed q1-memory-detailed q2-time-detailed q2-memory-detailed clean

help:
	@echo "Available commands:"
	@echo "  make help              - Show this help message"
	@echo "  make all              - Run all queries with both optimizations"
	@echo "  make q1-time          - Run query 1 with time optimization (simple output)"
	@echo "  make q1-memory        - Run query 1 with memory optimization (simple output)"
	@echo "  make q1-time-detailed - Run query 1 with detailed time profiling"
	@echo "  make q1-memory-detailed - Run query 1 with detailed memory profiling"
	@echo "  make q2-time          - Run query 2 with time optimization (simple output)"
	@echo "  make q2-memory        - Run query 2 with memory optimization (simple output)"
	@echo "  make q2-time-detailed - Run query 2 with detailed time profiling"
	@echo "  make q2-memory-detailed - Run query 2 with detailed memory profiling"
	@echo "  make clean            - Clean up temporary files"

# Query 1 - Simple Output
q1-time:
	@echo "Running Query 1 (Time Optimization)..."
	PYTHONPATH=. python tools/run.py --query q1 --optimization time

q1-memory:
	@echo "Running Query 1 (Memory Optimization)..."
	PYTHONPATH=. python tools/run.py --query q1 --optimization memory

# Query 1 - Detailed Profiling
q1-time-detailed:
	@echo "Running Query 1 (Detailed Time Profiling)..."
	PYTHONPATH=. python -m cProfile -s cumtime tools/run.py --query q1 --optimization time

q1-memory-detailed:
	@echo "Running Query 1 (Detailed Memory Profiling)..."
	PYTHONPATH=. python -m memory_profiler tools/run.py --query q1 --optimization memory

# Query 2 - Simple Output
q2-time:
	@echo "Running Query 2 (Time Optimization)..."
	PYTHONPATH=. python tools/run.py --query q2 --optimization time

q2-memory:
	@echo "Running Query 2 (Memory Optimization)..."
	PYTHONPATH=. python tools/run.py --query q2 --optimization memory

# Query 2 - Detailed Profiling
q2-time-detailed:
	@echo "Running Query 2 (Detailed Time Profiling)..."
	PYTHONPATH=. python -m cProfile -s cumtime tools/run.py --query q2 --optimization time

q2-memory-detailed:
	@echo "Running Query 2 (Detailed Memory Profiling)..."
	PYTHONPATH=. python -m memory_profiler tools/run.py --query q2 --optimization memory

# Query 3 - Simple Output
q3-time:
	@echo "Running Query 3 (Time Optimization)..."
	PYTHONPATH=. python tools/run.py --query q3 --optimization time

q3-memory:
	@echo "Running Query 3 (Memory Optimization)..."
	PYTHONPATH=. python tools/run.py --query q3 --optimization memory

all: clean q1-time q1-memory q2-time q2-memory q3-time q3-memory

clean:
	@echo "Cleaning up profiling files..."
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

.DEFAULT_GOAL := help