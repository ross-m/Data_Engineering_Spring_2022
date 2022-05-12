import pandas as pd

covid_data = pd.read_csv('COVID_county_data.csv')
census_data = pd.read_csv('acs2017_census_tract_data.csv', usecols=[
	'State',
	'County',
	'TotalPop',
	'IncomePerCap',
	'Poverty'
])

# Part A
census_data = census_data.assign(Total_Poverty=census_data['Poverty']*census_data['TotalPop'])
counties = census_data.groupby(['County', 'State'])
states = counties.sum().reset_index()['State']
cts = counties.sum().reset_index()['County']
pop = counties.sum().reset_index()['TotalPop']
incPerCap = counties.mean().reset_index()['IncomePerCap']
povPerc = counties.sum().reset_index()['Total_Poverty'] / pop

census_data = pd.DataFrame({
'County': cts,
'State': states,
'TotalPop': pop,
'IncomePerCap': incPerCap,
'Poverty': povPerc
})

print(census_data.loc[(census_data['County'] == 'Loudoun County') & (census_data['State'] == 'Virginia')])
print('\n')
print(census_data.loc[(census_data['County'] == 'Washington County') & (census_data['State'] == 'Oregon')])
print('\n')
print(census_data.loc[(census_data['County'] == 'Harlan County') & (census_data['State'] == 'Kentucky')])
print('\n')
print(census_data.loc[(census_data['County'] == 'Malheur County') & (census_data['State'] == 'Oregon')])
print('\n')
print(census_data.loc[census_data['TotalPop'] == census_data['TotalPop'].min()])
print('\n')
print(census_data.loc[census_data['TotalPop'] == census_data['TotalPop'].max()])
