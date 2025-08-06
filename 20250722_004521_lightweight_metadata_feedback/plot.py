import os
import json
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# Define which runs to plot and their labels
labels = {
    'run_0': 'Baseline',
    'run_1': 'Basic Column Mismatch',
    'run_2': 'Enhanced NLP Matching',
    'run_3': 'Execution Error Analysis',
    'run_4': 'Refined Error Feedback', 
    'run_5': 'Smarter Regeneration',
    'run_6': 'Enhanced Column Matching',
    'run_17': 'Domain Synonyms',
    'run_20': 'Improved Initial Prompt',
    'run_25': 'GROUP BY Guidance',
    'run_26': 'WHERE Guidance',
    'run_27': 'JOIN Guidance',
    'run_28': 'ORDER BY Guidance',
    'run_29': 'Final Combined'
}

def load_run_data(run_dir):
    """Load performance data from a run directory"""
    with open(os.path.join(run_dir, 'final_info.json')) as f:
        data = json.load(f)
    return data['bench']['means']

def plot_accuracy_comparison(run_dirs):
    """Plot easy_medium and total accuracy across runs"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    easy_medium = []
    total = []
    run_names = []
    
    for run_dir in run_dirs:
        data = load_run_data(run_dir)
        easy_medium.append(data['easy_medium'])
        total.append(data['total'])
        run_names.append(labels[os.path.basename(run_dir)])
    
    x = range(len(run_names))
    width = 0.35
    
    rects1 = ax.bar(x, easy_medium, width, label='Easy+Medium Accuracy')
    rects2 = ax.bar([p + width for p in x], total, width, label='Total Accuracy')
    
    ax.set_ylabel('Accuracy (%)')
    ax.set_title('Text-to-SQL Performance by Run')
    ax.set_xticks([p + width/2 for p in x])
    ax.set_xticklabels(run_names, rotation=45, ha='right')
    ax.legend()
    ax.set_ylim(60, 100)
    
    fig.tight_layout()
    plt.savefig('accuracy_comparison.png', bbox_inches='tight')
    plt.close()

def plot_score_distribution(run_dirs):
    """Plot the distribution of scores across buckets"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    buckets = ['not parsed', '0%', '(0-25%]', '(25-50%]', '(50-75%]', '(75-100%]']
    colors = ['#d62728', '#ff7f0e', '#1f77b4', '#2ca02c', '#9467bd', '#8c564b']
    
    for run_dir in run_dirs:
        data = load_run_data(run_dir)
        counts = data['counts']
        
        # Extract counts for each bucket
        values = []
        for bucket in buckets:
            if isinstance(counts[bucket], list):
                values.append(counts[bucket][0])
            else:
                values.append(counts[bucket])
        
        # Normalize to percentages
        total = sum(values)
        percentages = [v/total*100 for v in values]
        
        # Plot stacked bars
        bottom = 0
        for i, (bucket, color) in enumerate(zip(buckets, colors)):
            ax.bar(labels[os.path.basename(run_dir)], percentages[i], 
                  bottom=bottom, color=color, label=bucket if run_dir == run_dirs[0] else "")
            bottom += percentages[i]
    
    ax.set_ylabel('Percentage of Queries')
    ax.set_title('Score Distribution by Run')
    ax.legend(title='Score Buckets', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45, ha='right')
    fig.tight_layout()
    plt.savefig('score_distribution.png', bbox_inches='tight')
    plt.close()

def plot_improvement_timeline(run_dirs):
    """Plot the improvement timeline for key metrics"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    easy_medium = []
    total = []
    high_scores = []
    run_names = []
    
    for run_dir in run_dirs:
        data = load_run_data(run_dir)
        easy_medium.append(data['easy_medium'])
        total.append(data['total'])
        high_scores.append(data['counts']['(75-100%]'][0])
        run_names.append(labels[os.path.basename(run_dir)])
    
    x = range(len(run_names))
    
    ax.plot(x, easy_medium, marker='o', label='Easy+Medium Accuracy')
    ax.plot(x, total, marker='s', label='Total Accuracy')
    ax.plot(x, high_scores, marker='^', label='High-Scoring Queries (75-100%)')
    
    ax.set_xlabel('Experimental Run')
    ax.set_ylabel('Score/Count')
    ax.set_title('Performance Improvement Timeline')
    ax.set_xticks(x)
    ax.set_xticklabels(run_names, rotation=45, ha='right')
    ax.legend()
    ax.grid(True)
    
    fig.tight_layout()
    plt.savefig('improvement_timeline.png', bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    # Get all run directories that have labels
    run_dirs = [d for d in os.listdir() if os.path.isdir(d) and d in labels]
    run_dirs.sort(key=lambda x: list(labels.keys()).index(x))
    
    # Generate all plots
    plot_accuracy_comparison(run_dirs)
    plot_score_distribution(run_dirs)
    plot_improvement_timeline(run_dirs)
