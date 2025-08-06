import json
import os
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# Define which runs to plot and their labels
labels = {
    "run_0": "Baseline",
    "run_8": "M-schema",
    "run_9": "JOIN Guidance",
    "run_10": "Context JOINs",
    "run_14": "Aggregations",
    "run_16": "Simplified",
    "run_18": "Date Handling",
    "run_19": "Subqueries",
    "run_20": "CASE Statements",
    "run_21": "Complex JOINs",
    "run_22": "Window Functions"
}

def load_results():
    results = {}
    for run_dir in labels.keys():
        with open(f"{run_dir}/final_info.json") as f:
            data = json.load(f)
            results[run_dir] = data["bench"]["means"]
    return results

def plot_accuracy_progression(results):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Prepare data
    x = list(labels.values())
    easy_medium = [results[run]["easy_medium"] for run in labels]
    total = [results[run]["total"] for run in labels]
    
    # Plot lines
    ax.plot(x, easy_medium, marker='o', label='Easy+Medium Accuracy')
    ax.plot(x, total, marker='s', label='Total Accuracy')
    
    # Formatting
    ax.set_title('Accuracy Progression Across Experiment Runs')
    ax.set_ylabel('Accuracy (%)')
    ax.set_xlabel('Experiment Run')
    ax.set_ylim(80, 100)
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.legend()
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig('accuracy_progression.png')
    plt.close()

def plot_bucket_distribution(results):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Prepare data
    buckets = ['(75-100%]', '(50-75%]', '(25-50%]', '(0-25%]', '0%']
    colors = ['#2ecc71', '#27ae60', '#f39c12', '#e74c3c', '#c0392b']
    data = defaultdict(list)
    
    for run in labels:
        counts = results[run]["counts"]
        for bucket in buckets:
            if bucket in counts:
                data[bucket].append(counts[bucket][0])
            else:
                data[bucket].append(0)
    
    # Stacked bar plot
    bottom = np.zeros(len(labels))
    for bucket, color in zip(buckets, colors):
        ax.bar(labels.values(), data[bucket], bottom=bottom, label=bucket, color=color)
        bottom += np.array(data[bucket])
    
    # Formatting
    ax.set_title('Query Accuracy Distribution Across Runs')
    ax.set_ylabel('Number of Queries')
    ax.set_xlabel('Experiment Run')
    ax.legend(title='Accuracy Buckets', bbox_to_anchor=(1.05, 1))
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig('bucket_distribution.png')
    plt.close()

def plot_difficulty_breakdown(results):
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    difficulty_types = ['easy', 'medium', 'hard']
    
    for ax, diff_type in zip(axes, difficulty_types):
        # Prepare data
        high_acc = []
        mid_acc = []
        low_acc = []
        
        for run in labels:
            counts = results[run]["counts"]
            high = counts['(75-100%]'][1].get(diff_type, 0)
            mid = counts['(50-75%]'][1].get(diff_type, 0) + counts['(25-50%]'][1].get(diff_type, 0)
            low = counts['(0-25%]'][1].get(diff_type, 0) + counts['0%'][1].get(diff_type, 0)
            
            high_acc.append(high)
            mid_acc.append(mid)
            low_acc.append(low)
        
        # Stacked bar plot
        ax.bar(labels.values(), high_acc, label='High (75-100%)')
        ax.bar(labels.values(), mid_acc, bottom=high_acc, label='Medium (25-75%)')
        ax.bar(labels.values(), low_acc, bottom=np.array(high_acc)+np.array(mid_acc), label='Low (0-25%)')
        
        # Formatting
        ax.set_title(f'{diff_type.capitalize()} Queries')
        ax.set_ylabel('Number of Queries')
        ax.set_xticklabels(labels.values(), rotation=45, ha='right')
        ax.legend()
    
    fig.suptitle('Query Accuracy by Difficulty Level Across Runs')
    plt.tight_layout()
    plt.savefig('difficulty_breakdown.png')
    plt.close()

def main():
    results = load_results()
    plot_accuracy_progression(results)
    plot_bucket_distribution(results)
    plot_difficulty_breakdown(results)
    print("Plots generated: accuracy_progression.png, bucket_distribution.png, difficulty_breakdown.png")

if __name__ == "__main__":
    main()
