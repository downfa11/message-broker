# message-broker
Windows C++ 기반 메시지 브로커 직접 구현

<br>

- Non-Blocking IO : Windows 커널 오브젝트(IO Completion Port)를 이용한 proactor 비동기 통신
- Sequential IO : Network IO를 최소화하고자 기술적 도전중
- Zero-Copy : 데이터를 복사하지 않고 직접 버퍼를 통해 처리해서 메모리 비용 절감
- Buffer Pooling : 데이터 전송시 효율적인 Buffer 관리

<br>


'데이터의 논리적 구분과 물리적 분산을 명확히 구분'한다는 **Apache Kafka의 설계 철학에 충실한 메시지 브로커 개발**을 목표로 한다.

메시지 브로커의 클라이언트는 C++부터 구현해보고 점차 Java, Python, Go로 확장할 계획

중앙에서 metadata를 관리하는 기술을 구현할 역량이 부족하다고 판단, 일단 Broker를 중앙 서버에서 관리하도록 하고자 함.


