
def state_mapping(df):
    state_mapping = {
        'andaman-&-nicobar-islands': 'Andaman & Nicobar',
        'andhra-pradesh': 'Andhra Pradesh',
        'arunachal-pradesh': 'Arunachal Pradesh',
        'assam': 'Assam',
        'bihar': 'Bihar',
        'chandigarh': 'Chandigarh',
        'chhattisgarh': 'Chhattisgarh',
        'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
        'delhi': 'Delhi',
        'goa': 'Goa',
        'gujarat': 'Gujarat',
        'haryana': 'Haryana',
        'himachal-pradesh': 'Himachal Pradesh',
        'jammu-&-kashmir': 'Jammu & Kashmir',
        'jharkhand': 'Jharkhand',
        'karnataka': 'Karnataka',
        'kerala': 'Kerala',
        'ladakh': 'Ladakh',
        'lakshadweep': 'Lakshadweep',
        'madhya-pradesh': 'Madhya Pradesh',
        'maharashtra': 'Maharashtra',
        'manipur': 'Manipur',
        'meghalaya': 'Meghalaya',
        'mizoram': 'Mizoram',
        'nagaland': 'Nagaland',
        'odisha': 'Odisha',
        'puducherry': 'Puducherry',
        'punjab': 'Punjab',
        'rajasthan': 'Rajasthan',
        'sikkim': 'Sikkim',
        'tamil-nadu': 'Tamil Nadu',
        'telangana': 'Telangana',
        'tripura': 'Tripura',
        'uttar-pradesh': 'Uttar Pradesh',
        'uttarakhand': 'Uttarakhand',
        'west-bengal': 'West Bengal'
    }

    # Update the state names in the DataFrame
    df['State'] = df['State'].map(state_mapping)
    return df 