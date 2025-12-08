pluginManagement {
    plugins {
        val kotlinVersion = settings.extra["kotlin-lang.version"] as String
        kotlin("jvm") version kotlinVersion
        kotlin("plugin.spring") version kotlinVersion
        id("org.springframework.boot") version settings.extra["spring-boot.version"] as String
        id("io.spring.dependency-management") version settings.extra["spring-dm.version"] as String
    }
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "demo-kafka"
