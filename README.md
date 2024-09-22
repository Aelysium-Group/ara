# 👋 Meet ARA
ARA is a redundancy architecture for Java-level applications.
Originally build for [RustyConnector](https://github.com/Aelysium-Group/rustyconnector-core),
ARA decouples the connection between different modules allowing individual modules to be started, stopped, reloaded, etc without requiring the entire software to follow suit.

```gradle
maven {
    url "https://maven.mrnavastar.me/releases"
}
```

```gradle
implementation "group.aelysium:ara:1.0.0"
```

2024 © [Aelysium](https://aelysium.group)
