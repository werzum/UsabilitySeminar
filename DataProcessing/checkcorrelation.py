import numpy as np
import pandas as pd
import scipy.stats as st
import matplotlib.pyplot as plt

variable_spalte1 = ("en_raw_score_overall")
variable_spalte2 = ("user_id_count")

df = pd.read_csv("40,000_dataset_botometer_more_flags_no_duplicates_user_id_count.csv",sep=";",
                usecols=[variable_spalte1, variable_spalte2])

activity_greater_one = df[df['user_id_count'] > 1]
activity_greater_ten = df[df['user_id_count'] > 10]

#Korrelationen
corrMatrix = df.corr(method='pearson')
corr_with_p_value = st.pearsonr(df[variable_spalte1], df[variable_spalte2].fillna(0))
corrMatrix_activity_greater_one = activity_greater_one.corr(method='pearson')
print(corrMatrix)
print(corr_with_p_value)
print(corrMatrix_activity_greater_one)

x = df[variable_spalte1]
y = df[variable_spalte2]

#np.corrcoef(x)
#print(np.corrcoef)

plt.scatter(x, y)
#plt.title('A plot to show the correlation between ' + variable_spalte1 +' and '+ variable_spalte2)
plt.xlabel('English Astroturf Bot Score')
plt.ylabel('Activity Score (# of Tweets)')
#plt.plot(np.unique(x), np.poly1d(np.polyfit(x, y, 1))(np.unique(x)), color='yellow')
plt.show()

#print(np.corrcoef(x, y))