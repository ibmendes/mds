1. criar um database "sgs_bacen" no postgres 

1.1 Instalar o DBT 
python -m pip install dbt-core dbt-postgres

2. dbt init bacen_dbt
    3. conector postgres
    4. dbt debug (connection deve ser ok)~
5. cd bacen_dbt
6. dbt debug (deve ser ok)
7. dbt run 


models / path 

como testar o dbt? 

# compila e cria a view/tabela no banco
dbt run --select silver.variacao_indices

1. requisitos
uma instancia do postgres (localhost ou postgres no docker) porta 5432
2. dbt com postgres connector 

3. execute a modelagem
# primeiro entre na hierarquia do dbt (git bash ou wsl)
cd dbt_macro_modelling
dbt run -m variacao_indices
dbt run -m retorno_indices

4. ajustar host  do dbt no arquivo profiles.yml