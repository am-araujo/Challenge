# Challenge

Este documento apresentará a minha interpretação de alguns pontos que poderão ficar menos claros, mesmo que tenham sido colocados em comentário junto ao código.

## IDE e Run Configurations

O projeto foi compilado e executado no IntelliJ, com o Java 17 como SDK, ``` --add-exports java.base/sun.nio.ch=ALL-UNNAMED ``` como VM option e foi adicionado como Run Option ```"Add dependencies with "provided" scope to classpath"```.

## Interpretação de Exercício 

As situações que geraram maior dúvida foram identificadas no próprio código, sendo que aquilo que assumi é apresentado em comentário juntamente com o código. Nas tabelas em que a coluna apresenta como "Default Value = 0", assumi que o objetivo seria substituir os valores que se encontravam a NULL pelo valor 0. Outra situação que gerou dúvida foi o type da coluna ser do tipo Date, no entanto no exemplo o type é Timestamp, pelo que optei por seguir a implementação do tipo Date para a coluna "Last_Updated". Por último, na ordenação associada à parte 2, assumi que a ordenação seria associada ao rating e não necessariamente uma ordenação por ordem alfabética associada ao nome da App.
