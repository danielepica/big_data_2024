import matplotlib.pyplot as plt
import numpy as np

# Dataset
labels = ['1k', '10k', '100k', '1000k', '20000k']

# Job 1
mapreduce_local_job1 = [2.231, 2.5, 2.747, 7.764, 123]
mapreduce_cluster_job1 = [25.206, 25.181, 28.195, 30.268, 103.462]
hive_local_job1 = [33.026, 38.171, 43.198, 54.077, 203.336]
hive_cluster_job1 = [20.972, 21.374, 20.494, 41.411, 159.485]
spark_core_local_job1 = [3, 4, 4, 8, 61]
spark_core_cluster_job1 = [17, 17, 24, 36, 112]
spark_sql_local_job1 = [5, 5, 6, 9, 51]
spark_sql_cluster_job1 = [22, 24, 24, 28, 64]

# Job 2
mapreduce_local_job2 = [1.794, 1.822, 2.772, 7.799, 119]
mapreduce_cluster_job2 = [26.592, 25.274, 27.149, 31.505, 125.210]
hive_local_job2 = [18.106, 23.159, 28.13, 31.492, 110.03]
hive_cluster_job2 = [26.608, 25.317, 24.532, 32.784, 65.614]
spark_core_local_job2 = [3, 4, 5.2, 9, 120]
spark_core_cluster_job2 = [20, 20, 21, 37, 228]
spark_sql_local_job2 = [5, 6, 9, 23, 76]
spark_sql_cluster_job2 = [28, 29, 29, 35, 86]

# Job 3
mapreduce_local_job3 = [2.370, 3.102, 3.777, 6.523, 139.251]
mapreduce_cluster_job3 = [26.143, 25.363, 25.442, 29.367, 124.484]
hive_local_job3 = [34.233, 37.168, 50.842, 53.911, 95.427]
hive_cluster_job3 = [19.212, 15.353, 21.606, 19.69, 39.072]
spark_core_local_job3 = [4, 4, 5, 6, 45]
spark_core_cluster_job3 = [18, 19, 20, 28, 82]
spark_sql_local_job3 = [5, 5, 6, 9, 15]
spark_sql_cluster_job3 = [24, 24, 25, 25, 32]

# Function to create bar plot for a job
def plot_job_comparison(job_num, local_times, cluster_times, title):
    technologies = ['MapReduce', 'Hive', 'Spark Core', 'Spark SQL']
    local_means = [local_times[0], local_times[1], local_times[2], local_times[3]]
    cluster_means = [cluster_times[0], cluster_times[1], cluster_times[2], cluster_times[3]]

    x = np.arange(len(technologies))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, local_means, width, label='Local')
    rects2 = ax.bar(x + width/2, cluster_means, width, label='Cluster')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Times (s)')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(technologies)
    ax.legend()

    fig.tight_layout()

    plt.show()

# Plotting for each job
plot_job_comparison(1,
                    [mapreduce_local_job1[-1], hive_local_job1[-1], spark_core_local_job1[-1], spark_sql_local_job1[-1]],
                    [mapreduce_cluster_job1[-1], hive_cluster_job1[-1], spark_core_cluster_job1[-1], spark_sql_cluster_job1[-1]],
                    'Job 1 - Execution Times All Dataset')
plot_job_comparison(1,
                    [mapreduce_local_job1[0], hive_local_job1[0], spark_core_local_job1[0], spark_sql_local_job1[0]],
                    [mapreduce_cluster_job1[0], hive_cluster_job1[0], spark_core_cluster_job1[0], spark_sql_cluster_job1[0]],
                    'Job 1 - Execution Times 1k')
plot_job_comparison(1,
                    [mapreduce_local_job1[2], hive_local_job1[2], spark_core_local_job1[2], spark_sql_local_job1[2]],
                    [mapreduce_cluster_job1[2], hive_cluster_job1[2], spark_core_cluster_job1[2], spark_sql_cluster_job1[2]],
                    'Job 1 - Execution Times 100k')

plot_job_comparison(2,
                    [mapreduce_local_job2[-1], hive_local_job2[-1], spark_core_local_job2[-1], spark_sql_local_job2[-1]],
                    [mapreduce_cluster_job2[-1], hive_cluster_job2[-1], spark_core_cluster_job2[-1], spark_sql_cluster_job2[-1]],
                    'Job 2 - Execution Times All Dataset')
plot_job_comparison(2,
                    [mapreduce_local_job2[0], hive_local_job2[0], spark_core_local_job2[0], spark_sql_local_job2[0]],
                    [mapreduce_cluster_job2[0], hive_cluster_job2[0], spark_core_cluster_job2[0], spark_sql_cluster_job2[0]],
                    'Job 2 - Execution Times 1k')
plot_job_comparison(2,
                    [mapreduce_local_job2[2], hive_local_job2[2], spark_core_local_job2[2], spark_sql_local_job2[2]],
                    [mapreduce_cluster_job2[2], hive_cluster_job2[2], spark_core_cluster_job2[2], spark_sql_cluster_job2[2]],
                    'Job 2 - Execution Times 100k')

plot_job_comparison(3,
                    [mapreduce_local_job3[-1], hive_local_job3[-1], spark_core_local_job3[-1], spark_sql_local_job3[-1]],
                    [mapreduce_cluster_job3[-1], hive_cluster_job3[-1], spark_core_cluster_job3[-1], spark_sql_cluster_job3[-1]],
                    'Job 3 - Execution Times All Dataset')

plot_job_comparison(3,
                    [mapreduce_local_job3[0], hive_local_job3[0], spark_core_local_job3[0], spark_sql_local_job3[0]],
                    [mapreduce_cluster_job3[0], hive_cluster_job3[0], spark_core_cluster_job3[0], spark_sql_cluster_job3[0]],
                    'Job 3 - Execution Times 1k')

plot_job_comparison(3,
                    [mapreduce_local_job3[2], hive_local_job3[2], spark_core_local_job3[2], spark_sql_local_job3[2]],
                    [mapreduce_cluster_job3[2], hive_cluster_job3[2], spark_core_cluster_job3[2], spark_sql_cluster_job3[2]],
                    'Job 3 - Execution Times 100k')



# Creazione del grafico a linee
'''plt.figure(figsize=(10, 6))

# Aggiungi le linee per ogni tecnologia
plt.plot(dataset_sizes, mapreduce_local_times_job3, marker='o', label='MapReduce (locale)')
#plt.plot(dataset_sizes, mapreduce_cluster_times, marker='o', label='MapReduce (cluster)')
plt.plot(dataset_sizes, hive_local_times_job3, marker='o', label='Hive (locale)')
plt.plot(dataset_sizes, spark_core_local_times_job3, marker='o', label='Spark-Core (locale)')
plt.plot(dataset_sizes, spark_sql_local_times_job3, marker='o', label='Spark-SQL (locale)')


# Aggiungi le linee per le altre tecnologie

# Personalizza il grafico
plt.title('Tempi di Esecuzione per Tecnologia e Dimensione Dataset - Job 3')
plt.xlabel('Dimensione del Dataset')
plt.ylabel('Tempo di Esecuzione (s)')
plt.xticks(rotation=45)
plt.grid(True)
plt.legend()


# Aggiungi etichette di valore sull'asse y per i valori specifici
y_ticks = [0, 5, 15,20, 30, 60, 80, 100, 120]  # Esempio di valori da aggiungere
plt.yticks(y_ticks)  # Usa le etichette calcolate per l'asse y

# Mostra il grafico
plt.tight_layout()
plt.show()'''

'''# Dati
frameworks = ['MapReduce', 'Hive', 'Spark Core', 'Spark SQL']
local_times = [139.251, 95.427, 45, 15]
cluster_times = [124.484, 39.072, 82, 32]

# Creazione del grafico
plt.figure(figsize=(10, 6))

# Plot delle curve
plt.plot(frameworks, local_times, marker='o', label='Local Execution')
plt.plot(frameworks, cluster_times, marker='o', label='Cluster Execution')

# Etichette degli assi e titolo
plt.xlabel('Framework')
plt.ylabel('Execution Time (s)')
plt.title('Comparison of Local and Cluster Execution Times ALL dataset')
plt.legend()
plt.grid(True)

# Mostra il grafico
plt.tight_layout()
plt.show()'''