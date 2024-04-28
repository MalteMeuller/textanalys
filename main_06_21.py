#importing packages
import pandas as pd
import numpy as np
import pyodbc
from pathlib import Path
import os
import socket
import sys

#stänger av output om man kör i Batchen
if socket.gethostname()[:3].upper() == "MBS":

     f = open(os.devnull,'w')

     sys.stdout = f


#setting up SQL connection
path='//micro.intra/projekt/P1016$/P1016_Gem/RA/Malte Meuller/AI'
os.chdir(path)
cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=MQ02\B; '
                      'Database=P1016; '
                      'Trusted_Connection=yes; ')
cursor = cnxn.cursor()


#skapar först två funktioner som sedan används nedan. Först måste man ladda funktionerna sen köra de. 
        
# %% DENNA FUNKTIONEN TAR FRAM LISTA FÖR VARJE ÅR ÖVER HUR AI-RELATERAT VARJE SKILL ÄR.

def flexible_measure(Year, list_keywords):
        
    # Keyword, använd unambiguous core skills. Let the data find related skills as in Babina et al. 
    keywords = list_keywords
    
    #fetch jobbannons for Year x
    Query = "SELECT  * FROM ifn_jobbannons_{}".format(Year)
    df = pd.read_sql(Query, cnxn)
    
    #lower case variables
    df.rename(columns=lambda x: x.lower(), inplace=True)
    df = df.rename(columns = {'p1016_lopnr_orgnr': 'firm'})
   
    #dropping obs with missing values for skills
    df = df[df.skills.notnull()]
    df= df.reset_index(drop=True) #reindex
    
    #ändra number of positions till 1 if 0 or missing
    df['number_of_vacancies']=df['number_of_vacancies'].fillna(1)
    df['number_of_vacancies']=df['number_of_vacancies'].replace(0,1)
    
    #hittar rätt index för vår variabel "skills" och "number of vacancies". Index varierar men variabel namn är samma, Se variabler (excelfil))
    ix=df.columns.get_loc("skills")
    ix_num= df.columns.get_loc("number_of_vacancies")
    Total_ads = df["number_of_vacancies"].sum()
    print("Total ads",Year,":", Total_ads)

    
    #empty dataset to fill with values,
    #räknar hur många gånger en skill finns med samt hur många gånger den finns med tillsammans med core skills. 
    skills_df = pd.DataFrame(columns=('skill', 'count_co_occ', 'tot_count')) 
    
    #iterating over each row/jobb annons. 
    for i in range(0, len(df)): 
        
        #number of vacancies
        num_vac= df.iloc[i, ix_num] 
        num_vac= float(num_vac)
        
        #skills required
        skills= df.iloc[i, ix] # i=index för job, 1,2,3.. ix= index för skills (column nummer)
        skills= skills.replace("'", "") #cleaning list
        skills= skills.split(",") #splitting list
        skills=list(map(str.strip, skills)) #skapar en lista där alla skills är separerade
        skills=list(dict.fromkeys(skills)) #remove duplicates (om en skill nämns flera gånger)
        
        #add back to df in correct format
        skills_in ="', '".join(skills)
        skills_in="'"+skills_in+"'"
        df.iloc[i, ix]=skills_in
        
       #ser hur många av job ad skill som är core skills.  
        match=0 #räknar hur många matchningar som finns
        
        for s in skills: #för varje skills i en jobbannons
            if s in keywords: #om den även är en core skill
                match=match+1 #+1 på match, vi har alltså en match
            else:
                match=match+0 #om ingen match, + 0
         
        #om vi har en match, annonsen innehåller core skills
        if match>=1: 
            for s in skills: 
                
                #tar fram en lista med skills redan tillagda i vårt skills_df (final list) (undvika duplicates)
                temp_skills = list(skills_df['skill'])
                a=s #skill
                b=1 * num_vac #co_occourences(nämn samtidigt), multiplicerat med antal positioner/tar hänsyn till om det är flera jobb per annons
                c=1 * num_vac #totalt antal gånger nämnda
                
                #om skill s inte finns tillagd redan och det är match (dvs co-occouring)
                #då lägger vi till i listan den nya skillen samt adderar 1*antal_positioner till både co_occ och total_count
                if s not in temp_skills: 
                    append = {
                        "skill":a,
                        "count_co_occ": b,
                        "tot_count": c},
                    append = pd.DataFrame(append) #strukturerer om till df
                    skills_df = pd.concat([skills_df, append]) #appendar 
                    skills_df= skills_df.reset_index(drop=True) #reindex
        
                #om skill s redan finns i listan över skills så adderar vi endast poängen: 1*antal_positioner till både co_occ och total_count
                # håller koll på hu många gånger allt nämns. 
                else: 
                    #hittar positionen för skills=s, används för att imputera på rätt plats
                    position = skills_df.index[skills_df["skill"]==s].tolist()[0]
                    skills_df.iloc[position,1]= skills_df.iloc[position,1] + b #co_occ , adderar poäng
                    skills_df.iloc[position,2]= skills_df.iloc[position,2] + c #total count, adderar poäng
                    
        #match=0, annonsen innehåller inte core skills. Poäng läggs bara till på total count. 
        else : 
            for s in skills:
                temp_skills = list(skills_df['skill'])
                a=s
                b=0  # 0 eftersom det inte längre är co-occourences. 
                c=1 * num_vac
                
                if s not in temp_skills:
                    append = {
                        "skill":s,
                        "count_co_occ": b,
                        "tot_count": c},
                    append = pd.DataFrame(append)
                    skills_df = pd.concat([skills_df, append])
                    skills_df= skills_df.reset_index(drop=True) #reindex
                    
                else: #finns redan
                    position = skills_df.index[skills_df["skill"]==s].tolist()[0]
                    skills_df.iloc[position,2]= skills_df.iloc[position,2] + c #total count
                  
         
    #skapar det totala datasetet med ai-relatedness score för alla skills i datasetet.  
    skills_df['score']=skills_df['count_co_occ']/skills_df['tot_count']
    skills_df= skills_df.sort_values(by=['score'], ascending=[False])
    return skills_df
  
# %% DENNA FUNKTIONEN SKAPAR AD-LEVEL AI-RELATEDNESS GENOM ATT ANVÄNDA DE SCORES SOM VARJE SKILL HAR.
# Skapar två mått. Score_flex (datadriven approach som i Babina et al) och score_fixed (som i Acemoglu). Kan ses som broad och narrow.     

def score_job(Year, list_keywords, skill_list):
    
    #flexible measure
    #hämtar den gemensamma skill list på alla skills och score från all data. 
    final_grouped = pd.read_csv(skill_list) #ändra till år
    keywords = list_keywords #hämtar core skills
    
    #fixed measure, sätter up dataframe 
    data={'skill': keywords}
    core_skills=pd.DataFrame(data)
    core_skills['score']=1 #ger alla core skills =1
    
    #retreieve data from SQL
    Query = "SELECT * FROM ifn_jobbannons_{}".format(Year)
    df = pd.read_sql(Query, cnxn)
    df.rename (columns=lambda x: x.lower(), inplace=True) #lower case
    df = df.rename(columns = {'p1016_lopnr_orgnr': 'firm'}) #rename
    
    #droppa alla som inte har skills
    df = df[df.skills.notnull()]
    df= df.reset_index(drop=True) #reindex
    
    #ändra number of positions till 1 om 0 or missing
    df['number_of_vacancies']=df['number_of_vacancies'].fillna(1)
    df['number_of_vacancies']=df['number_of_vacancies'].replace(0,1)
    
    #hittar rätt index columns för skills
    ix=df.columns.get_loc("skills")
    ix_num= df.columns.get_loc("number_of_vacancies")
    #hittar index för occupation
    ox=df.columns.get_loc("occupation_name")
    orx= df.columns.get_loc("related_occupations")
    
    #sätter upp tom counter för att räkna score
    score_flexible=[]
    score_fixed = []
    for i in range(0, len(df)): 
        
        #om den saknar occupation tittar vi på om det finns för related occupation och sätter dit den
        occupation_org=df.iloc[i, ox]
        related_occupations=df.iloc[i, orx]
        if occupation_org=="nan" and related_occupations is not None:
            occ= df.iloc[i, orx] # i=index för job, 1,2,3.. ix= index för skills (column nummer)
            occ= occ.replace("'", "") #cleaning list
            occ= occ.split(",") #splitting list
            occ=list(map(str.strip, occ)) #skapar en lista där alla skills är separerade
            occ=occ[0]
            #add back to df in correct format
            df.iloc[i,ox]=occ
    
        #retreiving list of skills for job ad and turning it to df for merge
        skills= df.iloc[i, ix]
        skills= skills.replace("'", "")
        skills= skills.split(",")
        skills=list(map(str.strip, skills))
        skills=list(dict.fromkeys(skills)) #remove duplicates
        
        #lägger tillbaka till df
        skills_in ="', '".join(skills)
        skills_in="'"+skills_in+"'"
        df.iloc[i, ix]=skills_in
        
        skills = pd.DataFrame(skills)
        skills= skills.rename(columns={0: 'skill'}) #rename to fit
        
        #merging job add skills with skills from final list, Då får vi fram en lista med score för varje skill som nämns i annonsen
        merge= pd.merge(final_grouped, skills, on='skill', how='inner')
        meansc=merge['score'].mean() #retrieving score on job add by taking mean of all scores. 
        score_flexible.append(meansc) #appending to list
        
        #merging with fixed score. ger 1 om annonsen har core skill, 0 om den inte har. 
        merge_fix= pd.merge(core_skills, skills, on='skill', how='inner')
        meansc_fix=merge_fix['score'].mean() #retrieving score on job ad (alla core skills=1)
        if meansc_fix!=1: 
            meansc_fix=0
        score_fixed.append(meansc_fix) #appending to list
        
    df['score_flex']=score_flexible
    df['score_fixed']=score_fixed
    return df

# %%
###############################################################################
# MAIN SCRIPT
###############################################################################

function1=0
do_comb_list=0
function2=1
start_year=2018
end_year=2021
version="06_21"

### SKILL LIST ###
#här sätter man de åren som är relevanta
years = list(range(start_year, end_year+1,1))

#här sätter man keywords som man vill använda.Detta är alltså core skills.
list_keywords= ["AI", "Artificial intelligence", "Ml", "Maskininlarning",
                "Machine learning", "Computer vision", "Neural networks",
                "Neurala natverk", "Unsupervised learning", "Natural language processing"]

skill_list='//micro.intra/projekt/P1016$/P1016_Gem/RA/Malte Meuller/AI/data/'+version+'/datasets/skill_list_'+version+'.csv'


# %% # SKAPAR lista med skills och score, FÖRSTA FUNKTIONEN
if function1==1:
    for Year in years:
        print(Year)
        skills_df=flexible_measure(Year, list_keywords) #FUNKTIONEN, spottar ut lista över skills och dess AI-score för varje år
        skills_df.to_csv(f'//micro.intra/projekt/P1016$/P1016_Gem/RA/Malte Meuller/AI/data/skill_list/list_{Year}.csv', index=False) #sparas som csv.
     

#tom df för att skapa den totala listan
if do_comb_list==1:
    final= pd.DataFrame(columns=('skill', 'count_co_occ', 'tot_count', 'score')) 
    
    for Year in years:
        skills_df=pd.read_csv(f'//micro.intra/projekt/P1016$/P1016_Gem/RA/Malte Meuller/AI/data/skill_list/list_{Year}.csv')
        final= pd.concat([final, skills_df]) #appendas till gemensamma listan över skills och score
        
    #final dataset with scores. Collapsing by skill. 
    final_grouped = final.groupby('skill' ).agg({'count_co_occ': 'sum', 'tot_count': 'sum'}).reset_index()
    #skapar score_flexible
    final_grouped['score']=final_grouped['count_co_occ']/final_grouped['tot_count'] #antal gånger en skill nämns tillsammans med core skill / antal gånger den nämns totalt
    final_grouped=final_grouped.sort_values(by=['score'], ascending=False) #sorterar
    final_grouped= final_grouped[final_grouped['tot_count']>5] #ereases all that are mentioned less then 5 times
    
    #output, gemensam lista för alla år. 
    final_grouped.to_csv(skill_list, index=False)

    # %% Adderar score_flexible och score_fixed till jobbannonserna. ANDRA FUNTKIONEN 
if function2==1:  
    for Year in years:
        print("Second Function:")
        print(Year)
        df=score_job(Year, list_keywords, skill_list)
        
        #sorterar
        df=df.sort_values(by=['score_flex'], ascending=False)
        #flyttar columner
        p_sk=df.columns.get_loc("skills")
        p_sfl=df.columns.get_loc("score_flex")
        p_sfi=df.columns.get_loc("score_fixed")
        cols=df.columns.tolist()
        [cols.pop(p_sfi),cols.pop(p_sfl),cols.pop(p_sk)]
        cols.extend(['skills', 'score_flex', 'score_fixed'])
        df = df[cols]
        
        #output, jobbannon med score för varje år. Se stata för fortsättning. 
        Year_str=str(Year)
        df.to_csv(f'//micro.intra/projekt/P1016$/P1016_Gem/RA/Malte Meuller/AI/data/'+version+'/temp/jobb_'+Year_str+'.csv',
                  index=False)



